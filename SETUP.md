# Установка и Запуск

## Быстрый старт

### 1. Клонирование репозитория

```bash
git clone https://github.com/ваш-username/dyxless-bot.git
cd dyxless-bot
```

### 2. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 3. Настройка переменных окружения

Скопируйте файл с примером:

```bash
cp .env.example .env
```

Отредактируйте `.env` и добавьте ваши токены:

```
TELEGRAM_TOKEN=ваш_токен_от_BotFather
DYXLESS_API_TOKEN=ваш_токен_dyxless
```

**Как получить токены:**

- **TELEGRAM_TOKEN**: @BotFather → /mybots → выберите бота → API Token
- **DYXLESS_API_TOKEN**: @dyxless_bot → получить через команды бота

### 4. Создание необходимых директорий

```bash
mkdir -p temp checkpoints
```

### 5. Запуск бота

```bash
python3 bot.py
```

---

## Установка на BotHost.ru

Подробная инструкция в файле [BOTHOST_INSTALL.md](BOTHOST_INSTALL.md)

---

## Разработка

### Требования

- Python 3.8+
- pip

### Зависимости

- aiogram==2.25.1 - Telegram Bot API
- aiohttp==3.9.1 - Асинхронные HTTP запросы
- pandas==2.1.4 - Обработка данных
- openpyxl==3.1.2 - Работа с Excel

### Структура проекта

```
dyxless-bot/
├── bot.py              # Основной код бота
├── requirements.txt    # Зависимости
├── .env.example        # Пример переменных окружения
├── .env               # Ваши токены (не в Git!)
├── .gitignore         # Исключения для Git
├── README.md          # Документация
├── LICENSE            # Лицензия
├── BOTHOST_INSTALL.md # Инструкция для BotHost
├── temp/              # Временные файлы (создаётся автоматически)
└── checkpoints/       # Чекпоинты (создаётся автоматически)
```

---

## Безопасность

⚠️ **ВАЖНО:**

1. **Никогда** не коммитьте файл `.env` в Git
2. Файл `.gitignore` уже настроен для защиты
3. Используйте только переменные окружения для токенов
4. Private репозиторий защищает ваш код

---

## Поддержка

Для вопросов по установке смотрите:
- [README.md](README.md) - основная документация
- [BOTHOST_INSTALL.md](BOTHOST_INSTALL.md) - установка на хостинг
