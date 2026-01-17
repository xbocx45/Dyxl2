import logging
import os
import pandas as pd
import aiohttp
import asyncio
import time
import pickle
import hashlib
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import InputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.dispatcher import FSMContext
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment

# ==== Настройки ====
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '')
DYXLESS_API_TOKEN = os.getenv('DYXLESS_API_TOKEN', '')
DYXLESS_API_URL = 'https://api-dyxless.cfd/query'

# Проверка токенов
if not TELEGRAM_TOKEN or not DYXLESS_API_TOKEN:
    print("❌ Ошибка: Не установлены переменные окружения")
    print("Установите их командами:")
    print("export TELEGRAM_TOKEN='ваш_токен_telegram'")
    print("export DYXLESS_API_TOKEN='ваш_токен_dyxless'")
    exit(1)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

bot = Bot(token=TELEGRAM_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Создаём директории для чекпоинтов
os.makedirs("temp", exist_ok=True)
os.makedirs("checkpoints", exist_ok=True)

# Глобальный трекер активных задач
active_tasks = {}

# ==== Rate Limiter с паузой 16 минут ====
class SimpleRateLimiter:
    def __init__(self, max_calls=100, wait_minutes=16):
        self.max_calls = max_calls
        self.wait_seconds = wait_minutes * 60
        self.call_count = 0
        self.reset_time = None
        self.lock = asyncio.Lock()
        self.cycle_start_time = None
    
    async def wait_if_needed(self, user_id=None, progress_callback=None):
        """Проверяет лимит и ждёт при необходимости"""
        async with self.lock:
            self.call_count += 1
            
            if self.call_count == 1:
                self.reset_time = time.time()
                self.cycle_start_time = time.time()
                logging.info(f"🟢 Начат новый цикл запросов (лимит: {self.max_calls})")
                return
            
            if self.call_count > self.max_calls:
                elapsed = time.time() - self.reset_time
                wait_time = self.wait_seconds - elapsed
                
                if wait_time > 0:
                    logging.warning(f"🔴 Достигнут лимит {self.max_calls} запросов!")
                    logging.info(f"⏳ Ожидание {wait_time/60:.1f} минут до сброса лимита...")
                    
                    if user_id and progress_callback:
                        await progress_callback(
                            f"⏸️ Достигнут лимит API ({self.max_calls} запросов).\n"
                            f"⏳ Пауза на {int(wait_time/60)} минут {int(wait_time%60)} секунд...\n"
                            f"Обработка продолжится автоматически."
                        )
                    
                    # Ждём с обновлением прогресса каждую минуту
                    while wait_time > 0:
                        sleep_chunk = min(60, wait_time)
                        await asyncio.sleep(sleep_chunk)
                        wait_time -= sleep_chunk
                        
                        if wait_time > 0 and progress_callback:
                            minutes_left = int(wait_time / 60)
                            seconds_left = int(wait_time % 60)
                            await progress_callback(
                                f"⏳ Осталось ждать: {minutes_left} мин {seconds_left} сек..."
                            )
                
                self.call_count = 1
                self.reset_time = time.time()
                logging.info(f"✅ Лимит сброшен. Новый цикл из {self.max_calls} запросов")
                
                if progress_callback:
                    await progress_callback("✅ Пауза завершена. Продолжаем обработку...")
    
    def get_status(self):
        """Возвращает информацию о текущем статусе лимита"""
        if self.call_count == 0:
            return {
                'used': 0,
                'remaining': self.max_calls,
                'next_reset_minutes': 0,
                'cycle_time': 0
            }
        
        remaining = max(0, self.max_calls - self.call_count)
        
        if self.reset_time:
            elapsed = time.time() - self.reset_time
            next_reset = max(0, (self.wait_seconds - elapsed) / 60)
        else:
            next_reset = 0
        
        cycle_time = time.time() - self.cycle_start_time if self.cycle_start_time else 0
        
        return {
            'used': self.call_count,
            'remaining': remaining,
            'next_reset_minutes': round(next_reset, 1),
            'cycle_time': cycle_time
        }
    
    def estimate_time(self, total_requests):
        """Оценка времени выполнения с учётом пауз"""
        if total_requests <= 0:
            return 0
        
        full_cycles = total_requests // self.max_calls
        request_time = total_requests * 1
        pause_time = full_cycles * self.wait_seconds if full_cycles > 0 else 0
        
        total_seconds = request_time + pause_time
        return total_seconds

rate_limiter = SimpleRateLimiter(max_calls=100, wait_minutes=16)

# ==== Checkpoint Manager ====
class CheckpointManager:
    """Управление чекпоинтами для возобновления обработки"""
    
    def __init__(self, checkpoint_dir="checkpoints"):
        self.checkpoint_dir = checkpoint_dir
        os.makedirs(checkpoint_dir, exist_ok=True)
    
    def create_checkpoint_id(self, user_id, file_name):
        """Создаёт уникальный ID для чекпоинта"""
        data = f"{user_id}_{file_name}_{int(time.time())}"
        return hashlib.md5(data.encode()).hexdigest()[:12]
    
    def save_checkpoint(self, checkpoint_id, data):
        """Сохраняет чекпоинт"""
        checkpoint_path = os.path.join(self.checkpoint_dir, f"{checkpoint_id}.pkl")
        try:
            with open(checkpoint_path, 'wb') as f:
                pickle.dump(data, f)
            logging.info(f"💾 Чекпоинт сохранён: {checkpoint_id}")
            return True
        except Exception as e:
            logging.error(f"Ошибка сохранения чекпоинта: {e}")
            return False
    
    def load_checkpoint(self, checkpoint_id):
        """Загружает чекпоинт"""
        checkpoint_path = os.path.join(self.checkpoint_dir, f"{checkpoint_id}.pkl")
        try:
            if os.path.exists(checkpoint_path):
                with open(checkpoint_path, 'rb') as f:
                    data = pickle.load(f)
                logging.info(f"📂 Чекпоинт загружен: {checkpoint_id}")
                return data
            return None
        except Exception as e:
            logging.error(f"Ошибка загрузки чекпоинта: {e}")
            return None
    
    def delete_checkpoint(self, checkpoint_id):
        """Удаляет чекпоинт"""
        checkpoint_path = os.path.join(self.checkpoint_dir, f"{checkpoint_id}.pkl")
        try:
            if os.path.exists(checkpoint_path):
                os.remove(checkpoint_path)
                logging.info(f"🗑️ Чекпоинт удалён: {checkpoint_id}")
        except Exception as e:
            logging.error(f"Ошибка удаления чекпоинта: {e}")
    
    def save_partial_results(self, checkpoint_id, df, output_path):
        """Сохраняет частичные результаты в Excel"""
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Results')
                
                worksheet = writer.sheets['Results']
                
                for col in worksheet.columns:
                    max_length = 0
                    column = col[0].column_letter
                    for cell in col:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 60)
                    worksheet.column_dimensions[column].width = adjusted_width
                
                for row in worksheet.iter_rows():
                    for cell in row:
                        cell.alignment = Alignment(wrap_text=True, vertical='top')
            
            logging.info(f"💾 Частичные результаты сохранены: {output_path}")
            return True
        except Exception as e:
            logging.error(f"Ошибка сохранения частичных результатов: {e}")
            return False

checkpoint_manager = CheckpointManager()

# ==== FSM States ====
class SearchStates(StatesGroup):
    waiting_for_search_type = State()
    waiting_for_file = State()
    waiting_for_single_query = State()

# ==== Генератор прогресс-бара ====
def create_progress_bar(current, total, length=20):
    """Создаёт текстовый прогресс-бар"""
    if total == 0:
        percent = 0
    else:
        percent = (current / total) * 100
    
    filled = int(length * current / total) if total > 0 else 0
    bar = '█' * filled + '░' * (length - filled)
    return f"[{bar}] {percent:.1f}%"

def format_time(seconds):
    """Форматирует секунды в читаемый вид"""
    if seconds < 60:
        return f"{int(seconds)} сек"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        secs = int(seconds % 60)
        return f"{minutes} мин {secs} сек"
    else:
        hours = int(seconds / 3600)
        minutes = int((seconds % 3600) / 60)
        return f"{hours} ч {minutes} мин"

# ==== Асинхронный запрос к Dyxless API ====
async def dyxless_query(query: str, query_type: str = 'standart', user_id: int = None, progress_callback=None):
    """Выполняет асинхронный запрос к Dyxless API с контролем лимита"""
    
    await rate_limiter.wait_if_needed(user_id, progress_callback)
    
    payload = {
        "token": DYXLESS_API_TOKEN,
        "query": query,
        "type": query_type
    }
    
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                DYXLESS_API_URL,
                json=payload,
                headers={"Content-Type": "application/json"}
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    
                    status = rate_limiter.get_status()
                    logging.info(
                        f"✅ Запрос выполнен '{query}'. "
                        f"Использовано: {status['used']}/{rate_limiter.max_calls}"
                    )
                    
                    return result
                elif response.status == 402:
                    return {"status": False, "message": "insufficient balance"}
                else:
                    logging.error(f"HTTP {response.status} для запроса '{query}'")
                    return {"status": False, "message": f"HTTP ошибка: {response.status}"}
                    
    except Exception as e:
        logging.error(f"Ошибка запроса к API: {e}")
        return {"status": False, "message": f"Ошибка соединения: {str(e)}"}

# ==== Извлечение телефонов ====
def extract_phones_from_result(data: dict) -> str:
    """Извлекает телефоны из результата поиска Dyxless"""
    phones = set()
    
    if not data.get("status") or data.get("counts", 0) == 0:
        return ""
    
    for item in data.get("data", []):
        for key, value in item.items():
            if 'phone' in key.lower() or 'телефон' in key.lower():
                if isinstance(value, list):
                    phones.update(str(v) for v in value if v)
                elif value:
                    phones.add(str(value))
            
            if isinstance(value, str) and len(value) >= 10:
                import re
                phone_matches = re.findall(r'[+]?[7-8][\d\s\-\(\)]{10,}', value)
                phones.update(phone_matches)
    
    return ", ".join(sorted(phones)) if phones else ""

# ==== Форматирование результата ====
def format_full_result(data: dict) -> str:
    """Форматирует полный результат поиска"""
    if not data.get("status"):
        return f"ошибка: {data.get('message', 'неизвестная ошибка')}"
    
    if data.get("counts", 0) == 0:
        return ""
    
    result_parts = []
    
    for idx, item in enumerate(data.get("data", []), 1):
        item_parts = []
        table_name = item.pop('table_name', 'Неизвестно')
        item_parts.append(f"База: {table_name}")
        
        for key, value in item.items():
            if value and value != "" and value != []:
                if isinstance(value, list):
                    if len(value) > 0:
                        list_str = ", ".join(str(v) for v in value[:3])
                        if len(value) > 3:
                            list_str += f" и ещё {len(value) - 3}"
                        item_parts.append(f"{key}: {list_str}")
                else:
                    item_parts.append(f"{key}: {value}")
        
        result_parts.append(" | ".join(item_parts))
    
    return " || ".join(result_parts)

# ==== Фоновая обработка файла ====
async def process_file_background(user_id: int, file_path: str, file_name: str, checkpoint_id: str):
    """Фоновая обработка файла с чекпоинтами"""
    
    try:
        # Читаем файл
        if file_name.endswith('.xlsx'):
            df = pd.read_excel(file_path, dtype={'Результат (ИНН)': str})
        else:
            df = pd.read_csv(file_path, dtype={'Результат (ИНН)': str})
        
        if 'Результат (ИНН)' not in df.columns:
            await bot.send_message(user_id, "❌ Не найдена колонка 'Результат (ИНН)'")
            return
        
        df['Результат (ИНН)'] = df['Результат (ИНН)'].astype(str).str.strip()
        
        # Проверяем наличие чекпоинта
        checkpoint_data = checkpoint_manager.load_checkpoint(checkpoint_id)
        
        if checkpoint_data:
            # Возобновляем с чекпоинта
            phone_list = checkpoint_data['phone_list']
            full_list = checkpoint_data['full_list']
            cache = checkpoint_data['cache']
            start_index = checkpoint_data['processed']
            balance_exhausted = checkpoint_data.get('balance_exhausted', False)
            
            await bot.send_message(
                user_id,
                f"♻️ Возобновляю обработку с позиции {start_index}/{len(df)}"
            )
        else:
            # Начинаем с нуля
            phone_list = []
            full_list = []
            cache = {}
            start_index = 0
            balance_exhausted = False
        
        total_rows = len(df)
        unique_inns = df['Результат (ИНН)'].nunique()
        
        # Оценка времени
        remaining_requests = unique_inns - len(cache)
        estimated_seconds = rate_limiter.estimate_time(remaining_requests)
        estimated_time_str = format_time(estimated_seconds)
        
        # Уведомление о начале
        if start_index == 0:
            info_text = (
                f"📊 <b>Анализ файла:</b>\n\n"
                f"• Всего строк: <b>{total_rows}</b>\n"
                f"• Уникальных ИНН: <b>{unique_inns}</b>\n"
                f"• Примерное время: <b>{estimated_time_str}</b>\n\n"
                f"⚙️ <b>Особенности обработки:</b>\n"
                f"• Автосохранение каждые 50 запросов\n"
                f"• 💾 Отправка файла в чат каждые 100 запросов\n"
                f"• Можно безопасно перезапустить бота\n"
                f"• Все промежуточные файлы сохранены в чате\n\n"
                f"⏳ Начинаю обработку...\n"
                f"Вы будете получать обновления прогресса."
            )
            await bot.send_message(user_id, info_text, parse_mode="HTML")
        
        processed = start_index
        start_time = time.time()
        progress_msg = None
        last_update_time = time.time()
        last_checkpoint_save = 0
        last_file_send = 0  # Трекер последней отправки файла
        
        # Callback для обновления прогресса
        async def update_pause_status(text):
            nonlocal progress_msg
            try:
                if progress_msg:
                    await progress_msg.edit_text(text, parse_mode="HTML")
                else:
                    progress_msg = await bot.send_message(user_id, text, parse_mode="HTML")
            except Exception as e:
                logging.error(f"Ошибка обновления статуса: {e}")
        
        # Обрабатываем с позиции start_index
        for idx in range(start_index, total_rows):
            inn = df.loc[idx, 'Результат (ИНН)']
            
            if balance_exhausted:
                phone_list.append("нет денег на балансе")
                full_list.append("нет денег на балансе")
                processed += 1
                continue
            
            if not (inn.isdigit() and len(inn) in [10, 12]):
                phone_list.append("это не ИНН")
                full_list.append("это не ИНН")
                processed += 1
                continue
            
            # Проверяем кэш
            if inn in cache:
                phones, full_text, is_balance_error = cache[inn]
                if is_balance_error:
                    balance_exhausted = True
                    phone_list.append("нет денег на балансе")
                    full_list.append("нет денег на балансе")
                else:
                    phone_list.append(phones)
                    full_list.append(full_text)
            else:
                try:
                    result = await dyxless_query(inn, 'standart', user_id, update_pause_status)
                    
                    if "insufficient balance" in str(result.get('message', '')).lower():
                        balance_exhausted = True
                        cache[inn] = ("нет денег на балансе", "нет денег на балансе", True)
                        phone_list.append("нет денег на балансе")
                        full_list.append("нет денег на балансе")
                    else:
                        phones = extract_phones_from_result(result)
                        full_text = format_full_result(result)
                        
                        phones_display = phones if phones else "нет телефонов"
                        full_display = full_text if full_text else "ничего не найдено"
                        
                        cache[inn] = (phones_display, full_display, False)
                        phone_list.append(phones_display)
                        full_list.append(full_display)
                    
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logging.exception(f"Ошибка при запросе для ИНН {inn}")
                    cache[inn] = ("ошибка при запросе", "ошибка при запросе", False)
                    phone_list.append("ошибка при запросе")
                    full_list.append("ошибка при запросе")
            
            processed += 1
            
            # Сохраняем чекпоинт каждые 50 запросов
            if processed - last_checkpoint_save >= 50:
                checkpoint_data = {
                    'phone_list': phone_list,
                    'full_list': full_list,
                    'cache': cache,
                    'processed': processed,
                    'balance_exhausted': balance_exhausted
                }
                checkpoint_manager.save_checkpoint(checkpoint_id, checkpoint_data)
                last_checkpoint_save = processed
                
                # Сохраняем частичные результаты
                temp_df = df.iloc[:processed].copy()
                temp_df['Телефоны'] = phone_list
                temp_df['Всё'] = full_list
                partial_path = f"temp/partial_{checkpoint_id}.xlsx"
                checkpoint_manager.save_partial_results(checkpoint_id, temp_df, partial_path)
            
            # 💾 ОТПРАВЛЯЕМ ФАЙЛ В ЧАТ КАЖДЫЕ 100 ЗАПРОСОВ
            if processed - last_file_send >= 100 and processed > 0:
                try:
                    # Создаём временный файл с текущими результатами
                    temp_df = df.iloc[:processed].copy()
                    temp_df['Телефоны'] = phone_list
                    temp_df['Всё'] = full_list
                    
                    backup_path = f"temp/backup_{checkpoint_id}_{processed}.xlsx"
                    checkpoint_manager.save_partial_results(checkpoint_id, temp_df, backup_path)
                    
                    # Отправляем файл пользователю
                    backup_caption = (
                        f"💾 <b>Автосохранение #{processed // 100}</b>\n\n"
                        f"📊 Обработано: <b>{processed}/{total_rows}</b>\n"
                        f"🔍 Уникальных запросов: <b>{len(cache)}</b>\n"
                        f"💰 Оплачено запросов: <b>{len(cache)} × 2₽ = {len(cache) * 2}₽</b>\n\n"
                        f"✅ Файл сохранён в чате!\n"
                        f"⚡ Обработка продолжается..."
                    )
                    
                    await bot.send_document(
                        user_id,
                        InputFile(backup_path),
                        caption=backup_caption,
                        parse_mode="HTML"
                    )
                    
                    last_file_send = processed
                    logging.info(f"📤 Отправлен backup файл для позиции {processed}")
                    
                    # Удаляем старый backup файл (оставляем только последний)
                    if processed > 200:
                        old_backup = f"temp/backup_{checkpoint_id}_{processed - 100}.xlsx"
                        if os.path.exists(old_backup):
                            os.remove(old_backup)
                    
                except Exception as e:
                    logging.error(f"Ошибка отправки backup файла: {e}")
            
            # Обновляем прогресс
            current_time = time.time()
            if current_time - last_update_time >= 5 or processed == total_rows:
                try:
                    progress_percent = (processed / total_rows) * 100
                    elapsed_time = current_time - start_time
                    
                    if processed > start_index:
                        avg_time = elapsed_time / (processed - start_index)
                        remaining = total_rows - processed
                        estimated_remaining = avg_time * remaining
                    else:
                        estimated_remaining = 0
                    
                    limit_status = rate_limiter.get_status()
                    
                    progress_bar = create_progress_bar(processed, total_rows)
                    progress_text = (
                        f"📊 <b>Прогресс обработки:</b>\n\n"
                        f"{progress_bar}\n"
                        f"📈 Обработано: <b>{processed}/{total_rows}</b> ({progress_percent:.1f}%)\n"
                        f"🔍 Уникальных запросов: <b>{len(cache)}</b>\n\n"
                        f"⏱️ Затрачено: <b>{format_time(elapsed_time)}</b>\n"
                        f"⏳ Осталось: <b>{format_time(estimated_remaining)}</b>\n\n"
                        f"📡 <b>Лимит API:</b>\n"
                        f"• Использовано: {limit_status['used']}/{rate_limiter.max_calls}\n"
                        f"• Осталось: {limit_status['remaining']}\n\n"
                        f"💾 Автосохранение активно"
                    )
                    
                    if progress_msg:
                        await progress_msg.edit_text(progress_text, parse_mode="HTML")
                    else:
                        progress_msg = await bot.send_message(user_id, progress_text, parse_mode="HTML")
                    
                    last_update_time = current_time
                except Exception as e:
                    logging.error(f"Ошибка обновления прогресса: {e}")
        
        # Завершаем обработку
        df['Телефоны'] = phone_list
        df['Всё'] = full_list
        
        output_path = f"temp/result_{checkpoint_id}.xlsx"
        
        if file_name.endswith('.xlsx'):
            checkpoint_manager.save_partial_results(checkpoint_id, df, output_path)
        else:
            df.to_csv(output_path, index=False)
        
        # Отправляем финальный результат
        total_time = time.time() - start_time
        caption = (
            f"✅ <b>Обработка завершена!</b>\n\n"
            f"📊 Всего строк: <b>{total_rows}</b>\n"
            f"✅ Обработано: <b>{processed}</b>\n"
            f"💾 Уникальных запросов: <b>{len(cache)}</b>\n"
            f"💰 Общая стоимость: <b>{len(cache)} × 2₽ = {len(cache) * 2}₽</b>\n"
            f"⏱️ Общее время: <b>{format_time(total_time)}</b>\n\n"
            f"📥 ФИНАЛЬНЫЙ результат в файле ниже ⬇️"
        )
        
        await bot.send_document(user_id, InputFile(output_path), caption=caption, parse_mode="HTML")
        
        # Удаляем чекпоинт и временные файлы
        checkpoint_manager.delete_checkpoint(checkpoint_id)
        
        if progress_msg:
            try:
                await progress_msg.delete()
            except:
                pass
        
        # Удаляем из активных задач
        if user_id in active_tasks:
            del active_tasks[user_id]
        
        # Удаляем все backup файлы
        try:
            for f in os.listdir("temp"):
                if f.startswith(f"backup_{checkpoint_id}_"):
                    os.remove(os.path.join("temp", f))
        except Exception as e:
            logging.error(f"Ошибка удаления backup файлов: {e}")
        
    except Exception as e:
        logging.exception("Ошибка при обработке файла")
        await bot.send_message(user_id, f"⚠️ Ошибка: {str(e)}")
    
    finally:
        # Очистка
        if os.path.exists(file_path):
            os.remove(file_path)
        if 'output_path' in locals() and os.path.exists(output_path):
            os.remove(output_path)
        partial_path = f"temp/partial_{checkpoint_id}.xlsx"
        if os.path.exists(partial_path):
            os.remove(partial_path)

# ==== /start ====
@dp.message_handler(commands=['start'], state='*')
async def start_handler(message: types.Message, state: FSMContext):
    await state.finish()
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton(text="📊 Поиск по файлу с ИНН", callback_data="search_file_inn"),
        InlineKeyboardButton(text="🔍 Одиночный поиск", callback_data="single_search"),
        InlineKeyboardButton(text="📈 Статус лимита API", callback_data="check_limit"),
        InlineKeyboardButton(text="💰 Проверить баланс", callback_data="check_balance"),
        InlineKeyboardButton(text="ℹ️ Помощь", callback_data="help")
    )
    
    welcome_text = (
        "👋 <b>Добро пожаловать в Dyxless Search Bot!</b>\n\n"
        "📊 <b>Поиск по файлу с ИНН</b> - до 10 000+ контактов\n"
        "🔍 <b>Одиночный поиск</b> - быстрый поиск\n"
        "📈 <b>Статус лимита API</b> - мониторинг\n\n"
        "✨ <b>Защита ваших денег:</b>\n"
        "💾 Автосохранение каждые 50 запросов\n"
        "📤 Отправка файла в чат каждые 100 запросов\n"
        "♻️ Возобновление после сбоя\n"
        "🔒 Фоновая обработка без потерь\n\n"
        "⚠️ <b>Лимит API:</b> 100 запросов / 15 мин\n"
        "Выберите действие:"
    )
    
    await message.answer(welcome_text, reply_markup=keyboard, parse_mode="HTML")

# ==== Проверка статуса лимита ====
@dp.callback_query_handler(lambda c: c.data == "check_limit", state='*')
async def check_limit_handler(callback_query: types.CallbackQuery):
    status = rate_limiter.get_status()
    
    text = (
        f"📈 <b>Статус лимита API:</b>\n\n"
        f"✅ Использовано: <b>{status['used']}</b>\n"
        f"🔄 Осталось: <b>{status['remaining']}</b>\n"
        f"📊 Максимум: <b>{rate_limiter.max_calls}</b>\n\n"
    )
    
    if status['next_reset_minutes'] > 0 and status['used'] > rate_limiter.max_calls:
        text += f"⏰ Сброс через: <b>{status['next_reset_minutes']:.1f} мин</b>\n"
    elif status['used'] > 0:
        text += f"✅ Лимит активен\n"
    else:
        text += f"🟢 Лимит не использован\n"
    
    text += f"\n⏱️ Время цикла: {format_time(status['cycle_time'])}"
    
    await bot.send_message(callback_query.from_user.id, text, parse_mode='HTML')
    await callback_query.answer()

# ==== Проверка баланса ====
@dp.callback_query_handler(lambda c: c.data == "check_balance", state='*')
async def check_balance_handler(callback_query: types.CallbackQuery):
    await bot.send_message(
        callback_query.from_user.id,
        "💰 Для проверки баланса используйте /balance в @dyxless_bot"
    )
    await callback_query.answer()

# ==== Помощь ====
@dp.callback_query_handler(lambda c: c.data == "help", state='*')
async def help_handler(callback_query: types.CallbackQuery):
    help_text = (
        "📖 <b>Инструкция:</b>\n\n"
        "<b>📊 Поиск по файлу с ИНН:</b>\n"
        "• Поддержка до 10 000+ контактов\n"
        "• 💾 Автосохранение каждые 50 запросов\n"
        "• 📤 Отправка файла каждые 100 запросов\n"
        "• ♻️ Возобновление после сбоя\n"
        "• 🚀 Фоновая обработка\n\n"
        "<b>💰 Защита ваших денег:</b>\n"
        "• Каждый запрос = 2₽\n"
        "• Файлы сохраняются в чат каждые 100 запросов\n"
        "• При сбое - не теряете оплаченные запросы\n"
        "• Все промежуточные результаты у вас в чате\n\n"
        "<b>🔍 Одиночный поиск:</b>\n"
        "• Стандартный: телефон, email, ИНН (2₽)\n"
        "• Telegram: @username или ID (10₽)\n\n"
        "<b>⚠️ Лимиты API:</b>\n"
        "• 100 запросов / 15 минут\n"
        "• Пауза 16 минут при превышении\n"
        "• Автоматическое продолжение\n\n"
        "Используйте /start для меню"
    )
    
    await bot.send_message(callback_query.from_user.id, help_text, parse_mode="HTML")
    await callback_query.answer()

# ==== Поиск по файлу ====
@dp.callback_query_handler(lambda c: c.data == "search_file_inn", state='*')
async def search_file_inn_handler(callback_query: types.CallbackQuery, state: FSMContext):
    await state.update_data(search_mode="inn")
    
    instruction_text = (
        "📊 <b>Поиск по файлу с ИНН</b>\n\n"
        "<b>Формат:</b>\n"
        "• Excel (.xlsx) или CSV (.csv)\n"
        "• Колонка: <code>Результат (ИНН)</code>\n\n"
        "<b>💰 Защита ваших денег:</b>\n"
        "• 📤 Файл отправляется в чат каждые 100 запросов\n"
        "• 💾 Автосохранение каждые 50 запросов\n"
        "• 🔒 При сбое - не теряете оплаченные запросы\n"
        "• ♻️ Можно перезапустить с любого места\n\n"
        "<b>✨ Возможности:</b>\n"
        "• До 10 000+ контактов\n"
        "• Фоновая обработка\n"
        "• Безопасный перезапуск\n\n"
        "⏱️ <b>Примерное время:</b>\n"
        "• 100 контактов: ~2 минуты\n"
        "• 1000 контактов: ~3 часа\n"
        "• 10000 контактов: ~29 часов\n\n"
        "📤 Отправьте файл\n"
        "/cancel для отмены"
    )
    
    await bot.send_message(callback_query.from_user.id, instruction_text, parse_mode="HTML")
    await SearchStates.waiting_for_file.set()
    await callback_query.answer()

# ==== Обработка файла ====
@dp.message_handler(content_types=types.ContentType.DOCUMENT, state=SearchStates.waiting_for_file)
async def handle_file(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    # Проверяем, нет ли активной задачи
    if user_id in active_tasks:
        await message.answer(
            "⚠️ У вас уже есть активная задача обработки.\n"
            "Дождитесь её завершения или перезапустите бота."
        )
        return
    
    file = message.document
    file_name = file.file_name
    
    if not (file_name.endswith('.xlsx') or file_name.endswith('.csv')):
        await message.answer("❌ Поддерживаются только .xlsx и .csv")
        return
    
    file_path = f"temp/{user_id}_{file_name}"
    await file.download(destination_file=file_path)
    await message.answer("📥 Файл получен. Запускаю фоновую обработку...")
    
    # Создаём checkpoint ID
    checkpoint_id = checkpoint_manager.create_checkpoint_id(user_id, file_name)
    
    # Запускаем фоновую обработку
    active_tasks[user_id] = checkpoint_id
    asyncio.create_task(process_file_background(user_id, file_path, file_name, checkpoint_id))
    
    await state.finish()
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton(text="◀️ Главное меню", callback_data="back_to_menu")
    )
    await message.answer(
        "✅ Обработка запущена в фоновом режиме.\n"
        "Вы можете использовать бота для других задач.",
        reply_markup=keyboard
    )

# ==== Одиночный поиск ====
@dp.callback_query_handler(lambda c: c.data == "single_search", state='*')
async def single_search_handler(callback_query: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton(text="📱 Стандартный (2₽)", callback_data="type_standart"),
        InlineKeyboardButton(text="💬 Telegram (10₽)", callback_data="type_telegram"),
        InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu")
    )
    
    await bot.send_message(callback_query.from_user.id, "🔍 Выберите тип:", reply_markup=keyboard)
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("type_"), state='*')
async def query_type_selected(callback_query: types.CallbackQuery, state: FSMContext):
    query_type = callback_query.data.replace("type_", "")
    await state.update_data(query_type=query_type, search_mode="single")
    
    if query_type == "standart":
        prompt = "📱 <b>Стандартный поиск (2₽)</b>\n\nВведите: телефон, email, ИНН, имя\n/cancel для отмены"
    else:
        prompt = "💬 <b>Telegram поиск (10₽)</b>\n\nВведите: @username или ID\n/cancel для отмены"
    
    await bot.send_message(callback_query.from_user.id, prompt, parse_mode="HTML")
    await SearchStates.waiting_for_single_query.set()
    await callback_query.answer()

@dp.message_handler(state=SearchStates.waiting_for_single_query)
async def process_single_query(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    query_type = user_data.get('query_type', 'standart')
    query = message.text.strip()
    
    wait_msg = await message.answer("🔍 Поиск...")
    
    result = await dyxless_query(query, query_type, message.from_user.id)
    
    if result.get("status") and result.get("counts", 0) > 0:
        response_text = f"✅ <b>Найдено: {result['counts']}</b>\n\n"
        
        for idx, item in enumerate(result.get("data", []), 1):
            response_text += f"📋 <b>#{idx}</b>\n"
            table_name = item.pop('table_name', 'Неизвестно')
            response_text += f"📊 База: {table_name}\n"
            
            for key, value in item.items():
                if value and value != "" and value != []:
                    display_key = key.replace('_', ' ').title()
                    if isinstance(value, list):
                        if len(value) > 0:
                            list_str = ", ".join(str(v) for v in value[:3])
                            if len(value) > 3:
                                list_str += f" +{len(value) - 3}"
                            response_text += f"  • {display_key}: {list_str}\n"
                    else:
                        response_text += f"  • {display_key}: {value}\n"
            response_text += "\n"
    else:
        response_text = f"❌ {result.get('message', 'Не найдено')}"
    
    await wait_msg.delete()
    
    if len(response_text) > 4096:
        for i in range(0, len(response_text), 4096):
            await message.answer(response_text[i:i+4096], parse_mode="HTML")
    else:
        await message.answer(response_text, parse_mode="HTML")
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton(text="🔍 Новый поиск", callback_data="single_search"),
        InlineKeyboardButton(text="◀️ Меню", callback_data="back_to_menu")
    )
    await message.answer("Что дальше?", reply_markup=keyboard)
    await state.finish()

# ==== Возврат в меню ====
@dp.callback_query_handler(lambda c: c.data == "back_to_menu", state='*')
async def back_to_menu(callback_query: types.CallbackQuery, state: FSMContext):
    await state.finish()
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton(text="📊 Поиск по файлу", callback_data="search_file_inn"),
        InlineKeyboardButton(text="🔍 Одиночный поиск", callback_data="single_search"),
        InlineKeyboardButton(text="📈 Статус API", callback_data="check_limit"),
        InlineKeyboardButton(text="💰 Баланс", callback_data="check_balance"),
        InlineKeyboardButton(text="ℹ️ Помощь", callback_data="help")
    )
    
    await bot.send_message(callback_query.from_user.id, "📋 Главное меню:", reply_markup=keyboard)
    await callback_query.answer()

# ==== /cancel ====
@dp.message_handler(commands=['cancel'], state='*')
async def cancel_handler(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("Нечего отменять")
        return
    
    await state.finish()
    await message.answer("❌ Отменено", reply_markup=types.ReplyKeyboardRemove())
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton(text="◀️ Меню", callback_data="back_to_menu"))
    await message.answer("Возврат:", reply_markup=keyboard)

# ==== Запуск ====
if __name__ == '__main__':
    print("🤖 Dyxless Bot Enterprise запущен...")
    print("✨ Поддержка больших файлов с автосохранением")
    logging.info("Dyxless Bot Enterprise started")
    executor.start_polling(dp, skip_updates=True)
