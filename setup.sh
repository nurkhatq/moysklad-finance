#!/bin/bash

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "=========================================="
echo "  МойСклад - Финансовый Отчёт"
echo "  Скрипт установки и настройки"
echo "=========================================="
echo ""

# Проверка Python
echo -e "${YELLOW}Проверка Python...${NC}"
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python 3 не установлен!${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Python найден: $(python3 --version)${NC}"
echo ""

# Создание виртуального окружения
echo -e "${YELLOW}Создание виртуального окружения...${NC}"
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo -e "${GREEN}✅ Виртуальное окружение создано${NC}"
else
    echo -e "${BLUE}ℹ️  Виртуальное окружение уже существует${NC}"
fi
echo ""

# Активация виртуального окружения
echo -e "${YELLOW}Активация виртуального окружения...${NC}"
source venv/bin/activate

# Обновление pip
echo -e "${YELLOW}Обновление pip...${NC}"
pip install --upgrade pip > /dev/null 2>&1
echo -e "${GREEN}✅ pip обновлён${NC}"
echo ""

# Установка зависимостей
echo -e "${YELLOW}Установка зависимостей...${NC}"
pip install -r requirements.txt
echo -e "${GREEN}✅ Зависимости установлены${NC}"
echo ""

# Создание .env файла
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}Создание .env файла...${NC}"
    cp .env.example .env
    echo -e "${GREEN}✅ .env файл создан${NC}"
    echo -e "${YELLOW}⚠️  Не забудьте отредактировать .env файл!${NC}"
else
    echo -e "${BLUE}ℹ️  .env файл уже существует${NC}"
fi
echo ""

# Проверка credentials.json
if [ ! -f "credentials.json" ]; then
    echo -e "${YELLOW}⚠️  Файл credentials.json не найден${NC}"
    echo -e "${YELLOW}📝 Инструкция:${NC}"
    echo "  1. Перейдите в Google Cloud Console"
    echo "  2. Создайте Service Account"
    echo "  3. Скачайте JSON ключ"
    echo "  4. Сохраните как credentials.json в текущей директории"
else
    echo -e "${GREEN}✅ credentials.json найден${NC}"
fi
echo ""

# Проверка конфигурации
if [ ! -f "config.json" ]; then
    echo -e "${YELLOW}Создание базового config.json...${NC}"
    cat > config.json << EOF
{
    "moysklad_token": "",
    "google_credentials_file": "credentials.json",
    "spreadsheet_name": "Финансовый отчёт",
    "sync_schedule": "daily",
    "sync_time": "09:00",
    "days_back": 30
}
EOF
    echo -e "${GREEN}✅ config.json создан${NC}"
    echo -e "${YELLOW}⚠️  Добавьте токен МойСклад в config.json${NC}"
else
    echo -e "${BLUE}ℹ️  config.json уже существует${NC}"
fi
echo ""

# Создание директории для GitHub Actions
if [ ! -d ".github/workflows" ]; then
    echo -e "${YELLOW}Создание структуры GitHub Actions...${NC}"
    mkdir -p .github/workflows
    echo -e "${GREEN}✅ Директория .github/workflows создана${NC}"
fi
echo ""

# Итоговая информация
echo "=========================================="
echo -e "${GREEN}✅ УСТАНОВКА ЗАВЕРШЕНА!${NC}"
echo "=========================================="
echo ""
echo -e "${BLUE}📋 Следующие шаги:${NC}"
echo ""
echo "1. Настройте credentials.json (Google Service Account)"
echo "2. Добавьте токен МойСклад в config.json"
echo "3. Запустите приложение:"
echo -e "   ${GREEN}streamlit run app.py${NC}"
echo ""
echo "4. Для автоматизации через GitHub:"
echo "   - Создайте репозиторий на GitHub"
echo "   - Добавьте Secrets (MOYSKLAD_TOKEN, GOOGLE_CREDENTIALS, etc.)"
echo "   - Скопируйте workflow файл в .github/workflows/"
echo ""
echo -e "${YELLOW}📖 Подробная инструкция в README.md${NC}"
echo ""
echo "=========================================="