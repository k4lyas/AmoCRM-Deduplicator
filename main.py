#эта версия на "мягкое удаление" дубликата
import os
import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Request
import uvicorn
import asyncio

load_dotenv()

AMO_DOMAIN = os.getenv("AMO_DOMAIN")                   #подгрузка API
AMO_ACCESS_TOKEN = os.getenv("AMO_ACCESS_TOKEN")

HEADERS = {
    "Authorization": f"Bearer {AMO_ACCESS_TOKEN}",
    "Content-Type": "application/json"
}
app = FastAPI (title='AmoCRM Deduplicator')  #название приложения (app) скрипта

async def get_new_contact_data(contact_id): #функция запроса данных профиля
    url = f"https://{AMO_DOMAIN}/api/v4/contacts/{contact_id}"
    TELEGRAM_FIELD_ID = 1612333
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=HEADERS)

    if response.status_code == 200:
        data = response.json()
        custom_fields = data.get ("custom_fields_values", [])
        telegram_username = None
        if custom_fields:
            for field in custom_fields:
                if field.get ("field_id") == TELEGRAM_FIELD_ID:
                    telegram_username = field.get("values")[0].get("value")
                    break
        print(f"У контакта {data.get('name')}найден ник: {telegram_username}")
        return {
                    "id": data.get("id"),
                    "telegram_username": telegram_username,
                    "description": f"Запрос от {data.get('name')}"
                }
    return None
async def duplicate_research(username): #функция поиска дубликатов нового профиля
        if not username:
            return None
        
        clean_username = username.replace("@", "")
        print(f"Выполняется поиск в AmoCRM по нику: {clean_username}")

        url = f"https://{AMO_DOMAIN}/api/v4/contacts?query={clean_username}"
        TELEGRAM_FIELD_ID = 1612333

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=HEADERS)

            if response.status_code == 200:
                contacts = response.json().get("_embedded", {}).get("contacts", [])

                for contact in contacts:
                    custom_fields = contact.get("custom_fields_values", [])
                    if custom_fields:
                        for field in custom_fields:
                            if field.get("field_id") == TELEGRAM_FIELD_ID:

                                val = field.get("values")[0].get("value")
                                if val.replace("@", "") == clean_username:
                                    print (f"Найден дубликат: {contact.get('name')}(ID: {contact.get('id')})")
                                    return {
                                        "id": contact.get("id"),
                                        "name": contact.get("name")
                                    }
                                
            elif response.status_code == 204:
                print (f"Совпадений по нику {clean_username}в базе нет")
            else:
                print (f"Ошибка API при поиске: {response.status_code}")

                return None
async def merge_and_delete(old_id, new_id, description): #функция слияния и удаления
    print (f"Перенос примечения {description} в старый профиль ID {old_id}...")
    print ("Ожидание генерации сделок AmoCRM. Осталось 2 секунды...")
    await asyncio.sleep(2) #пауза 2 секунды

    async with httpx.AsyncClient() as client:
            notes_url = f"https://{AMO_DOMAIN}/api/v4/contacts/{old_id}/notes"
            note_payload = [
            {
                "note_type": "common",
                "params": {
                    "text": f"⚠️ Склеен дубликат!\n{description}"
                }
            }
        ]
            note_resp = await client.post(notes_url, headers=HEADERS, json=note_payload)
            if note_resp.status_code in [200, 201]:
                print(f"Примечение успешно добавлено профиль {old_id}")
            else:
                print(f"Ошибка добавление примечания: {note_resp.status_code}")

            update_url = f"https://{AMO_DOMAIN}/api/v4/contacts"
            update_payload = [
                {
                    "id": int(new_id),
                    "name": "Дубль (На удаление)",
                    "custom_fields_values": [
                        {
                            "field_id": 1612333,
                            "values": [{"value": ''}]
                            }
                        ]
                    }
                ]
            update_resp = await client.patch (update_url, headers=HEADERS, json = update_payload)
            if update_resp.status_code == 200:
                print(f"Профиль {new_id} изолирован (данные удалены, статус изменен)")
            else:
                print(f"Ошибка изолирования профиля дубликата: {update_resp.status_code}")
            return True

@app.post('/webhook') #endpoint - дверь для amocrm
async def amo_webhook(request: Request): #функция приема вебхуков
    form_data = await request.form() #преобразование HTTP запроса в массив

    print ("Получен вебхук от AmoCRM")
    
    contact_id = form_data.get("contacts[add][0][id]") or form_data.get("contacts[update][0][id]")  #достаем данные из массива
    if contact_id:
        print(f"Получен контакт с ID {contact_id}")
        
        
        new_contact = await get_new_contact_data (contact_id) #поиск и получение данных от contact_id
        
        old_contact = await duplicate_research (new_contact["telegram_username"]) #получаем данные от contact id (функция сверху), берем username и отдаем username чтобы amocrm нашел дубликаты
        
        
        if old_contact and str(old_contact ['id']) != str(contact_id):
            print(f"Внимание! Найден дубликат! Старый ID: {old_contact['id']}")

            await merge_and_delete(old_contact['id'], contact_id, new_contact ['description'])

            print ('Операция слияния успешно завершена')
        
        else:
            print('Дубликатов нет. Создан контакт')

    else: print ("Не найден ID контакта. Выполняется поиск сырых данных:")
    print(form_data)
    

    return {"status": "success", 'message': "200 OK"} 
if __name__ == "__main__": #запуск файла вручную
    uvicorn.run('main:app', host = "0.0.0.0", port=8000, reload = True) 
    #main.app - указатель на запуск функции app = FastAPI и последующих в коде
    #host = "0.0.0.0" - сетевая настройка открывающая доступ во всю сеть чтобы amocrm мог достучаться до скрипта
    #port=8000 - порт для вебхуков
    #reload = True - автоматическая перезагрузка сервера после каждого обновления кода 