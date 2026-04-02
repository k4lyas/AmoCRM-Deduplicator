import os
import json
import logging
import httpx
import asyncio
from dotenv import load_dotenv
from fastapi import FastAPI, Request
import uvicorn

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {"level": record.levelname, "message": record.getMessage(), "module": record.module}
        return json.dumps(log_record, ensure_ascii=False)

logger = logging.getLogger("deduplicator")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
if not logger.handlers: logger.addHandler(handler)

load_dotenv(override=True) 
AMO_DOMAIN = os.getenv("AMO_DOMAIN")
AMO_ACCESS_TOKEN = os.getenv("AMO_ACCESS_TOKEN")
TG_FIELD_ID = int(os.getenv("TELEGRAM_FIELD_ID", 1404413))

HEADERS = {"Authorization": f"Bearer {AMO_ACCESS_TOKEN}", "Content-Type": "application/json"}
app = FastAPI(title='AmoCRM Deduplicator')

processing_contacts = set()

async def make_request(method, url, retries=3, **kwargs):
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.request(method, url, headers=HEADERS, timeout=15.0, **kwargs)
                if response.status_code == 204: 
                    return None
                
                if response.status_code >= 400:
                    if method == "DELETE" and response.status_code == 405:
                        return "METHOD_NOT_ALLOWED"
                    logger.error(f"Детали ошибки от Амо ({response.status_code}): {response.text}")
                    
                response.raise_for_status()
                return response.json() if response.text else None
        except Exception as e:
            if attempt == retries - 1:
                logger.error(f"Ошибка запроса {url}: {str(e)}")
                raise e # Прокидываем ошибку выше для жесткого контроля
            await asyncio.sleep(2 ** attempt)
    return None

async def get_new_contact_data(contact_id):
    url = f"https://{AMO_DOMAIN}/api/v4/contacts/{contact_id}"
    try:
        data = await make_request("GET", url)
    except Exception:
        return None
    
    if not data: return None
    if "Дубль" in data.get("name", ""):
        return "IS_DUPLICATE"

    cf = data.get("custom_fields_values") or []
    tg, phone = None, None
    for f in cf:
        if f.get("field_id") == TG_FIELD_ID: tg = f.get("values")[0].get("value")
        elif f.get("field_code") == "PHONE": phone = f.get("values")[0].get("value")
                
    return {
        "id": int(data.get("id")), 
        "name": data.get("name"), 
        "telegram_username": tg, 
        "phone": phone,
        "created_at": int(data.get("created_at", 0)) 
    }

async def duplicate_research(phone, telegram, current_id):
    duplicates = {}
    async def search(query_str):
        if not query_str: return
        clean = query_str.replace("@", "").replace("+", "").replace(" ", "").strip()
        if len(clean) < 3: return
        url = f"https://{AMO_DOMAIN}/api/v4/contacts?query={clean}"
        try:
            data = await make_request("GET", url)
            if data and "_embedded" in data:
                for c in data["_embedded"]["contacts"]:
                    c_id = int(c["id"])
                    if c_id != current_id and "Дубль" not in c.get("name", ""): 
                        duplicates[c_id] = c
        except Exception:
            pass

    await search(phone)
    await search(telegram)
    if not duplicates: return None
    return sorted(duplicates.values(), key=lambda x: x.get("created_at", float('inf')))[0]

def is_strict_match(old_contact, new_contact):
    # Жесткая валидация: очищаем от мусора и сравниваем
    def clean(val):
        return str(val).replace("+", "").replace("-", "").replace(" ", "").replace("@", "").strip().lower() if val else ""
    
    # Достаем телефон и ТГ деда
    old_tg, old_phone = "", ""
    cf = old_contact.get("custom_fields_values") or []
    for f in cf:
        if f.get("field_id") == TG_FIELD_ID: old_tg = clean(f.get("values")[0].get("value"))
        elif f.get("field_code") == "PHONE": old_phone = clean(f.get("values")[0].get("value"))

    new_phone = clean(new_contact.get('phone'))
    new_tg = clean(new_contact.get('telegram_username'))

    if new_phone and new_phone in old_phone: return True
    if old_phone and old_phone in new_phone: return True
    if new_tg and new_tg == old_tg: return True
    
    return False

async def transfer_notes(from_id, to_id):
    url = f"https://{AMO_DOMAIN}/api/v4/contacts/{from_id}/notes"
    try:
        notes_data = await make_request("GET", url)
    except Exception:
        return
        
    if not notes_data or "_embedded" not in notes_data: return
    
    notes = notes_data["_embedded"]["notes"]
    dest_url = f"https://{AMO_DOMAIN}/api/v4/contacts/{to_id}/notes"
    
    for note in notes:
        note_type = note.get("note_type", "unknown")
        # Вытаскиваем текст, даже если это звонок или системное сообщение
        text = note.get("params", {}).get("text") or str(note.get("params", {}))
        
        payload = [{"note_type": "common", "params": {"text": f"[{note_type}] {text}"}}]
        try:
            await make_request("POST", dest_url, json=payload)
        except Exception as e:
            logger.error(f"Не удалось перенести примечание {note['id']}: {e}")
            
    logger.info(f"Перенесено примечаний: {len(notes)}")

async def transfer_leads(from_id, to_id):
    url = f"https://{AMO_DOMAIN}/api/v4/contacts/{from_id}/links"
    try:
        links_data = await make_request("GET", url)
    except Exception:
        return True # Если связей нет, считаем успешным
        
    if not links_data or "_embedded" not in links_data: return True
    
    leads = [l for l in links_data["_embedded"]["links"] if l["to_entity_type"] == "leads"]
    
    for lead in leads:
        lead_id = lead["to_entity_id"]
        
        link_url = f"https://{AMO_DOMAIN}/api/v4/leads/{lead_id}/link"
        unlink_url = f"https://{AMO_DOMAIN}/api/v4/leads/{lead_id}/unlink"
        
        try:
            # Сначала пытаемся привязать к деду
            await make_request("POST", link_url, json=[{"to_entity_id": to_id, "to_entity_type": "contacts"}])
            # Если привязалось без ошибок - отвязываем от клона
            await make_request("POST", unlink_url, json=[{"to_entity_id": from_id, "to_entity_type": "contacts"}])
        except Exception as e:
            logger.error(f"Ошибка при переносе сделки {lead_id}. Останавливаем процесс удаления клона.")
            return False # Возвращаем False, чтобы отменить удаление
            
    logger.info(f"Успешно перепривязано сделок: {len(leads)}")
    return True

async def enrich_old_contact(old_contact, new_contact):
    cf = old_contact.get("custom_fields_values") or []
    old_tg, old_phone = None, None
    for f in cf:
        if f.get("field_id") == TG_FIELD_ID: old_tg = f.get("values")[0].get("value")
        elif f.get("field_code") == "PHONE": old_phone = f.get("values")[0].get("value")

    update_fields = []
    if not old_phone and new_contact.get('phone'):
        update_fields.append({"field_code": "PHONE", "values": [{"value": new_contact['phone']}]})
    if not old_tg and new_contact.get('telegram_username'):
        update_fields.append({"field_id": TG_FIELD_ID, "values": [{"value": new_contact['telegram_username']}]})

    if update_fields:
        url = f"https://{AMO_DOMAIN}/api/v4/contacts"
        payload = [{"id": old_contact["id"], "custom_fields_values": update_fields}]
        try:
            await make_request("PATCH", url, json=payload)
            logger.info(f"Обогатили деда новыми данными из клона.")
        except Exception:
            pass

async def strategy_delete(old_id, new_id):
    delete_url = f"https://{AMO_DOMAIN}/api/v4/contacts/{new_id}"
    try:
        res = await make_request("DELETE", delete_url)
        if res == "METHOD_NOT_ALLOWED":
            logger.info(f"DELETE запрещен. Делаем мягкое удаление (переименование).")
            update_url = f"https://{AMO_DOMAIN}/api/v4/contacts"
            payload = [{"id": new_id, "name": f"Дубль (ID {old_id})"}]
            await make_request("PATCH", update_url, json=payload)
        else:
            logger.info("Клон успешно удален физически.")
    except Exception as e:
        logger.error(f"Ошибка при удалении/переименовании клона: {e}")

async def merge_and_delete(old_contact, new_contact):
    old_id, new_id = old_contact['id'], new_contact['id']
    logger.info(f"Начинаем слияние {new_id} -> {old_id}")
    
    await enrich_old_contact(old_contact, new_contact)
    await transfer_notes(new_id, old_id)
    
    # Проверяем, успешно ли перенеслись сделки
    leads_transferred = await transfer_leads(new_id, old_id)
    
    if leads_transferred:
        await strategy_delete(old_id, new_id)
        logger.info(f"Слияние завершено успешно.")
    else:
        logger.warning("Слияние прервано: не удалось перенести все сделки, клон сохранен для безопасности.")

@app.post('/webhook')
async def amo_webhook(request: Request):
    try:
        form_data = await request.form()
        contact_id_raw = form_data.get("contacts[add][0][id]") or form_data.get("contacts[update][0][id]")
        if not contact_id_raw: 
            return {"status": "ignored"}
        
        contact_id = int(contact_id_raw)
        
        if contact_id in processing_contacts:
            logger.info(f"Вебхук отклонен: контакт {contact_id} уже в процессе.")
            return {"status": "already_processing"}
            
        processing_contacts.add(contact_id)
        
        try:
            new_contact = await get_new_contact_data(contact_id)
            if new_contact == "IS_DUPLICATE":
                return {"status": "already_processed"}
            if not new_contact: 
                return {"status": "error"}

            old_contact = await duplicate_research(new_contact['phone'], new_contact['telegram_username'], contact_id)
            
            if old_contact:
                if old_contact['created_at'] >= new_contact['created_at']:
                    logger.info("Это Оригинал. Отменяем обратное слияние.")
                else:
                    if is_strict_match(old_contact, new_contact):
                        logger.info(f"Найдено 100% совпадение с дедом (ID {old_contact['id']})! Запускаем перенос.")
                        await merge_and_delete(old_contact, new_contact)
                    else:
                        logger.info("Найден похожий контакт, но жесткая валидация провалилась (разные номера). Пропускаем.")
            else:
                logger.info("Дубли не найдены.")
                
            return {"status": "success"}
        finally:
            async def clear_lock():
                await asyncio.sleep(10)
                processing_contacts.discard(contact_id)
            asyncio.create_task(clear_lock())
            
    except Exception as e:
        logger.error(f"Ошибка вебхука: {str(e)}")
        return {"status": "error"}

if __name__ == "__main__":
    uvicorn.run('main:app', host="0.0.0.0", port=8000, reload=True)