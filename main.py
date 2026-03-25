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
                return None
            await asyncio.sleep(2 ** attempt)
    return None

async def get_new_contact_data(contact_id):
    url = f"https://{AMO_DOMAIN}/api/v4/contacts/{contact_id}"
    data = await make_request("GET", url)
    if not data: return None
    
    if "Дубль" in data.get("name", ""):
        return "IS_DUPLICATE"

    cf = data.get("custom_fields_values") or []
    
    # logger.info(f"ВСЕ ПОЛЯ КОНТАКТА {contact_id}: {json.dumps(cf, ensure_ascii=False)}") (так надо)
    
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
        data = await make_request("GET", url)
        if data and "_embedded" in data:
            for c in data["_embedded"]["contacts"]:
                c_id = int(c["id"])
                if c_id != current_id and "Дубль" not in c.get("name", ""): 
                    duplicates[c_id] = c

    await search(phone)
    await search(telegram)
    if not duplicates: return None
    return sorted(duplicates.values(), key=lambda x: x.get("created_at", float('inf')))[0]

async def transfer_notes(from_id, to_id):
    url = f"https://{AMO_DOMAIN}/api/v4/contacts/{from_id}/notes"
    notes_data = await make_request("GET", url)
    if not notes_data or "_embedded" not in notes_data: return
    
    notes = notes_data["_embedded"]["notes"]
    dest_url = f"https://{AMO_DOMAIN}/api/v4/contacts/{to_id}/notes"
    
    for note in notes:
        if note.get("note_type") == "common":
            payload = [{"note_type": "common", "params": {"text": note["params"]["text"]}}]
            await make_request("POST", dest_url, json=payload)
    logger.info(f"Перенесено примечаний: {len(notes)}")

async def transfer_leads(from_id, to_id):
    url = f"https://{AMO_DOMAIN}/api/v4/contacts/{from_id}/links"
    links_data = await make_request("GET", url)
    if not links_data or "_embedded" not in links_data: return
    
    leads = [l for l in links_data["_embedded"]["links"] if l["to_entity_type"] == "leads"]
    for lead in leads:
        lead_id = lead["to_entity_id"]
        
        link_url = f"https://{AMO_DOMAIN}/api/v4/leads/{lead_id}/link"
        await make_request("POST", link_url, json=[{"to_entity_id": to_id, "to_entity_type": "contacts"}])
        
        unlink_url = f"https://{AMO_DOMAIN}/api/v4/leads/{lead_id}/unlink"
        await make_request("POST", unlink_url, json=[{"to_entity_id": from_id, "to_entity_type": "contacts"}])
        
    logger.info(f"Перепривязано сделок: {len(leads)}")

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
        await make_request("PATCH", url, json=payload)
        logger.info(f"Обогатили деда новыми данными из клона.")

async def strategy_delete(old_id, new_id):
    delete_url = f"https://{AMO_DOMAIN}/api/v4/contacts/{new_id}"
    res = await make_request("DELETE", delete_url)
    
    if res == "METHOD_NOT_ALLOWED":
        logger.info(f"DELETE запрещен (тариф). Делаем мягкое удаление (переименование).")
        update_url = f"https://{AMO_DOMAIN}/api/v4/contacts"
        payload = [{"id": new_id, "name": f"Дубль (ID {old_id})"}]
        await make_request("PATCH", update_url, json=payload)
    else:
        logger.info("Клон успешно удален физически.")

async def merge_and_delete(old_contact, new_contact):
    old_id, new_id = old_contact['id'], new_contact['id']
    logger.info(f"Начинаем слияние {new_id} -> {old_id}")
    
    await enrich_old_contact(old_contact, new_contact)
    await transfer_notes(new_id, old_id)
    await transfer_leads(new_id, old_id)
    await strategy_delete(old_id, new_id)
    
    logger.info(f"Слияние завершено успешно.")

@app.post('/webhook')
async def amo_webhook(request: Request):
    try:
        form_data = await request.form()
        contact_id_raw = form_data.get("contacts[add][0][id]") or form_data.get("contacts[update][0][id]")
        if not contact_id_raw: 
            return {"status": "ignored"}
        
        contact_id = int(contact_id_raw)
        
        if contact_id in processing_contacts:
            logger.info(f"Вебхук отклонен: контакт {contact_id} уже в процессе обработки.")
            return {"status": "already_processing"}
            
        processing_contacts.add(contact_id)
        
        try:
            logger.info(f"Обрабатываем контакт ID: {contact_id}")
            new_contact = await get_new_contact_data(contact_id)
            
            if new_contact == "IS_DUPLICATE":
                logger.info("Контакт уже помечен как 'Дубль'. Останавливаемся.")
                return {"status": "already_processed"}
                
            if not new_contact: 
                return {"status": "error"}

            logger.info(f"Ищем дубли для: тел={new_contact['phone']}, тг={new_contact['telegram_username']}")
            old_contact = await duplicate_research(new_contact['phone'], new_contact['telegram_username'], contact_id)
            
            if old_contact:
                if old_contact['created_at'] >= new_contact['created_at']:
                    logger.info("Мы обрабатываем Оригинал (он старше найденного дубля). Отменяем обратное слияние.")
                else:
                    logger.info(f"Найден старый контакт ID {old_contact['id']}! Запускаем перенос.")
                    await merge_and_delete(old_contact, new_contact)
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