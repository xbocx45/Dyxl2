import aiohttp
import asyncio
import os

DYXLESS_API_TOKEN = os.getenv('DYXLESS_API_TOKEN', '')
DYXLESS_API_URL = 'https://api-dyxless.cfd/query'

async def test_api():
    """Тестирование API Dyxless"""
    
    payload = {
        "token": DYXLESS_API_TOKEN,
        "query": "7736207543",  # ИНН Яндекса для теста
        "type": "standart"
    }
    
    print(f"🔍 Тестирую API Dyxless...")
    print(f"📡 URL: {DYXLESS_API_URL}")
    print(f"🔑 Токен установлен: {'ДА' if DYXLESS_API_TOKEN else 'НЕТ'}")
    if DYXLESS_API_TOKEN:
        print(f"   Первые 10 символов: {DYXLESS_API_TOKEN[:10]}...")
    else:
        print(f"   ❌ ТОКЕН НЕ УСТАНОВЛЕН В ПЕРЕМЕННЫХ ОКРУЖЕНИЯ!")
    print(f"📦 Тестовый запрос: ИНН Яндекса (7736207543)\n")
    
    if not DYXLESS_API_TOKEN:
        print("⚠️ ОСТАНОВКА: Установите переменную DYXLESS_API_TOKEN")
        return
    
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            print("🌐 Отправка запроса к API...")
            async with session.post(
                DYXLESS_API_URL,
                json=payload,
                headers={"Content-Type": "application/json"}
            ) as response:
                
                print(f"\n📊 HTTP Status Code: {response.status}")
                print(f"📋 Content-Type: {response.headers.get('Content-Type', 'не указан')}")
                print(f"📏 Content-Length: {response.headers.get('Content-Length', 'не указан')} bytes\n")
                
                response_text = await response.text()
                print(f"📄 Полный ответ от API:")
                print("=" * 60)
                print(response_text[:1000])  # Первые 1000 символов
                print("=" * 60)
                print()
                
                if response.status == 200:
                    print(f"✅ ✅ ✅ API РАБОТАЕТ ОТЛИЧНО! ✅ ✅ ✅")
                    try:
                        result = await response.json()
                        print(f"🎯 Найдено записей: {result.get('counts', 0)}")
                        if result.get('data'):
                            print(f"📦 Данные получены из {len(result['data'])} источников")
                    except Exception as e:
                        print(f"⚠️ Ошибка парсинга JSON: {e}")
                        
                elif response.status == 401:
                    print(f"❌ ❌ ❌ ОШИБКА 401: НЕПРАВИЛЬНЫЙ ТОКЕН! ❌ ❌ ❌")
                    print(f"\n🔧 ЧТО ДЕЛАТЬ:")
                    print(f"   1. Откройте Telegram → @dyxless_bot")
                    print(f"   2. Получите новый API токен")
                    print(f"   3. Обновите переменную DYXLESS_API_TOKEN на BotHost")
                    print(f"   4. Перезапустите бота")
                    
                elif response.status == 402:
                    print(f"💰 💰 💰 ОШИБКА 402: НЕДОСТАТОЧНО СРЕДСТВ! 💰 💰 💰")
                    print(f"\n🔧 ЧТО ДЕЛАТЬ:")
                    print(f"   1. Откройте Telegram → @dyxless_bot")
                    print(f"   2. Отправьте команду /balance")
                    print(f"   3. Пополните баланс")
                    
                elif response.status == 404:
                    print(f"❌ ❌ ❌ ОШИБКА 404: API ENDPOINT НЕ НАЙДЕН! ❌ ❌ ❌")
                    print(f"\n🔧 ЧТО ДЕЛАТЬ:")
                    print(f"   1. Возможно URL API изменился")
                    print(f"   2. Проверьте актуальную документацию:")
                    print(f"      https://dyxless.b-cdn.net/api.html")
                    print(f"   3. Свяжитесь с поддержкой @dyxless_bot")
                    
                elif response.status == 429:
                    print(f"⏱️ ⏱️ ⏱️ ОШИБКА 429: ПРЕВЫШЕН ЛИМИТ ЗАПРОСОВ! ⏱️ ⏱️ ⏱️")
                    print(f"\n🔧 ЧТО ДЕЛАТЬ:")
                    print(f"   1. Подождите 16 минут")
                    print(f"   2. Лимит: 100 запросов за 15 минут")
                    
                else:
                    print(f"⚠️ ⚠️ ⚠️ НЕИЗВЕСТНАЯ ОШИБКА: HTTP {response.status} ⚠️ ⚠️ ⚠️")
                    print(f"\n📧 Отправьте этот вывод в поддержку")
                
    except aiohttp.ClientConnectionError as e:
        print(f"❌ ❌ ❌ ОШИБКА СОЕДИНЕНИЯ! ❌ ❌ ❌")
        print(f"   Детали: {e}")
        print(f"\n🔧 ЧТО ДЕЛАТЬ:")
        print(f"   1. Проверьте интернет-соединение")
        print(f"   2. Проверьте доступность домена:")
        print(f"      ping api-dyxless.cfd")
        print(f"   3. Возможно сервис временно недоступен")
        
    except asyncio.TimeoutError:
        print(f"⏱️ ⏱️ ⏱️ ТАЙМАУТ ЗАПРОСА! ⏱️ ⏱️ ⏱️")
        print(f"   API не ответил за 30 секунд")
        print(f"\n🔧 ЧТО ДЕЛАТЬ:")
        print(f"   1. Повторите попытку")
        print(f"   2. Если повторяется - сервис перегружен")
        
    except Exception as e:
        print(f"❌ ❌ ❌ НЕОЖИДАННАЯ ОШИБКА! ❌ ❌ ❌")
        print(f"   Тип: {type(e).__name__}")
        print(f"   Детали: {e}")
        import traceback
        print(f"\n📋 Полный traceback:")
        traceback.print_exc()

if __name__ == '__main__':
    print("=" * 60)
    print("   DYXLESS API DIAGNOSTIC TOOL")
    print("=" * 60)
    print()
    asyncio.run(test_api())
    print()
    print("=" * 60)
    print("   ТЕСТ ЗАВЕРШЁН")
    print("=" * 60)
