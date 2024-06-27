import os
import time
import requests
from datetime import datetime, timedelta
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Название директории с бэкапом
backup_directory = "slack_backup"
# ID каналов, где нужно сохранить только текст сообщений
channel_ids = [
    "G583A76F9"
]
# ID каналов, где нужно сохранить только файлы
channels_to_save_files = [
    "G583A76F9"
]
slack_token = 'token'
# Кол-во дней, за сколько нужно сделать бэкап
backup_days = 365 * 1
# Переключатель для сохранения файлов изображений (True - сохранять, False - не сохранять). Только для групповых чатов
save_images = False

# Инициализация Slack WebClient
client = WebClient(token=slack_token)


# Функция для создания папки, если она не существует
def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


# Функция для получения всех каналов (включая личные сообщения)
def get_all_channels(channel_ids=None):
    try:
        if channel_ids:
            channels = []
            for channel_id in channel_ids:
                try:
                    result = client.conversations_info(channel=channel_id)
                    channels.append(result["channel"])
                except SlackApiError as e:
                    print(f"Ошибка при получении информации о канале {channel_id}: {e.response['error']}")
            return channels
        else:
            result = client.conversations_list(types="public_channel,private_channel,im,mpim")
            channels = result["channels"]
            return channels

    except SlackApiError as e:
        print(f"Ошибка при получении списка каналов: {e.response['error']}")
        return []


# Функция для получения всех пользователей
def get_all_users():
    try:
        result = client.users_list()
        users = result["members"]
        return users
    except SlackApiError as e:
        print(f"Ошибка при получении списка пользователей: {e.response['error']}")
        return []


# Функция для получения имени пользователя по идентификатору
def get_username(user_id, users):
    for user in users:
        if user["id"] == user_id:
            return user["real_name"] if not user["is_bot"] and not user["deleted"] else user["name"]
    return "Unknown"


# Функция для получения имени пользователя для личных сообщений
def get_dm_user_name(channel_id, users):
    try:
        result = client.conversations_info(channel=channel_id)
        user_id = result["channel"]["user"]
        return get_username(user_id, users)
    except SlackApiError as e:
        print(f"Ошибка при получении имени пользователя для личного сообщения {channel_id}: {e.response['error']}")
        return "unknown_user"


# Функция для получения истории сообщений канала с учетом временного ограничения
def get_channel_history_with_files_and_threads(channel_id, save_files=False, folder_name=None, backup_days=backup_days):
    try:
        # Начинаем с пустого списка для хранения всех сообщений и вложенных файлов
        all_messages = []
        latest = None

        # Вычисляем дату backup_days назад от текущего момента
        backup_date_limit = datetime.now() - timedelta(days=backup_days)

        while True:
            # Получаем историю сообщений для заданного канала с вложенными файлами
            result = client.conversations_history(channel=channel_id, latest=latest, limit=1000, inclusive=True)
            messages = result["messages"]

            # Проверяем каждое сообщение
            for message in messages:
                message_ts = float(message["ts"])
                message_date = datetime.fromtimestamp(message_ts)

                # Если сообщение было отправлено до даты ограничения, прекращаем получение сообщений
                if message_date < backup_date_limit:
                    return all_messages

                # Добавляем сообщение в список
                all_messages.append(message)

                # Проверяем наличие вложенных файлов в сообщении
                if save_files and "files" in message:
                    save_files_to_folder(message["files"], channel_id, message_date, folder_name)

                # Проверяем наличие треда и загружаем его сообщения
                if "thread_ts" in message:
                    thread_ts = message["thread_ts"]
                    thread_messages = get_thread_messages(channel_id, thread_ts, save_files, folder_name, backup_days)
                    all_messages.extend(thread_messages)

            # Если больше нет сообщений или не осталось сообщений за последние backup_days, выходим из цикла
            if not messages or not result["has_more"]:
                break

            # Устанавливаем latest для следующего запроса
            latest = messages[-1]["ts"]

            # Для больших историй может потребоваться несколько запросов API
            # Задержка между запросами, чтобы не превышать ограничения API
            time.sleep(1)

        return all_messages

    except SlackApiError as e:
        if e.response["error"] == "ratelimited":
            print("Достигнут лимит запросов к API Slack. Повторная попытка через некоторое время...")
            time.sleep(5)  # Повторная попытка через 5 секунд
            return get_channel_history_with_files_and_threads(channel_id, save_files, folder_name, backup_days)
        else:
            print(f"Ошибка при получении истории сообщений: {e.response['error']}")
            return []

    except Exception as ex:
        print(f"Произошла ошибка: {ex}")
        return []


# Функция для получения сообщений в треде
def get_thread_messages(channel_id, thread_ts, save_files=False, folder_name=None, backup_days=backup_days):
    try:
        thread_messages = []
        latest = None

        while True:
            result = client.conversations_replies(channel=channel_id, ts=thread_ts, latest=latest, limit=1000,
                                                  inclusive=True)
            messages = result["messages"]

            for message in messages:
                message_ts = float(message["ts"])
                message_date = datetime.fromtimestamp(message_ts)

                # Если сообщение было отправлено до даты ограничения, прекращаем получение сообщений
                if message_date < (datetime.now() - timedelta(days=backup_days)):
                    return thread_messages

                message["is_thread"] = True  # Отмечаем сообщение как тред
                thread_messages.append(message)

                # Проверяем наличие вложенных файлов в сообщении
                if save_files and "files" in message:
                    save_files_to_folder(message["files"], channel_id, message_date, folder_name)

            if not messages or not result["has_more"]:
                break

            latest = messages[-1]["ts"]

            # Задержка между запросами, чтобы не превышать ограничения API
            time.sleep(1)

        return thread_messages

    except SlackApiError as e:
        if e.response["error"] == "ratelimited":
            print("Достигнут лимит запросов к API Slack. Повторная попытка через некоторое время...")
            time.sleep(30)  # Повторная попытка через 30 секунд
            return get_thread_messages(channel_id, thread_ts, save_files, folder_name, backup_days)
        else:
            print(f"Ошибка при получении сообщений треда: {e.response['error']}")
            return []

    except Exception as ex:
        print(f"Произошла ошибка: {ex}")
        return []


# Функция для сохранения вложенных файлов в указанную папку
def save_files_to_folder(files, channel_id, message_date, folder_name):
    try:
        # Используем имя папки для канала или личного сообщения
        folder_path = f"{backup_directory}/{folder_name}"
        create_directory(folder_path)

        headers = {"Authorization": f"Bearer {slack_token}"}
        timestamp = message_date.strftime('%Y-%m-%d %H-%M-%S')

        for file in files:
            try:
                if "url_private_download" not in file:
                    print(f"У файла {file['id']} отсутствует URL для скачивания.")
                    continue

                file_info = client.files_info(file=file["id"])
                file_type = file_info["file"]["mimetype"]

                # Пропускаем скачивание изображений для групповых чатов
                if not save_images and not channel_id.startswith("D"):
                    print(f"Пропускаем скачивание изображения {file['id']} ({file_info['file']['name']}).")
                    continue

                download_url = file_info["file"]["url_private_download"]
                file_name = file_info["file"]["name"]
                new_file_name = f"[{timestamp}] {file_name}"
                file_path = os.path.join(folder_path, new_file_name)

                if os.path.exists(file_path):
                    print(f"Файл {new_file_name} уже существует, пропускаем его сохранение.")
                    continue

                response = requests.get(download_url, headers=headers)
                if response.status_code == 200:
                    with open(file_path, "wb") as f:
                        f.write(response.content)
                else:
                    print(f"Ошибка при загрузке файла {file['id']}: статус {response.status_code}")

            except SlackApiError as e:
                print(f"Ошибка при сохранении файла {file['id']}: {e.response['error']}")

            except Exception as ex:
                print(f"Произошла ошибка при скачивании файла {file['id']}: {ex}")

    except SlackApiError as e:
        print(f"Ошибка при сохранении вложенных файлов: {e.response['error']}")


# Основная программа
def main():
    try:
        all_channels = get_all_channels(channel_ids)
        all_users = get_all_users()
        create_directory(backup_directory)

        for channel in all_channels:
            save_files = channel["id"] in channels_to_save_files
            folder_name = channel["name"] if not channel[
                "is_im"] else f"im-{get_dm_user_name(channel['id'], all_users)}"

            if channel["is_im"] and "im-" not in folder_name:
                continue

            print(f"Начался бэкап сообщений для канала {folder_name}")

            messages = get_channel_history_with_files_and_threads(channel["id"], save_files, folder_name)

            if messages:
                folder_path = f"{backup_directory}/{folder_name}"
                create_directory(folder_path)

                with open(f"{folder_path}/messages.txt", "a", encoding="utf-8") as f:
                    for message in messages:
                        try:
                            timestamp = datetime.fromtimestamp(float(message["ts"])).strftime('%d-%m-%Y %H:%M:%S')
                            author_name = get_username(message["user"], all_users) if "user" in message else "Slack Bot"
                            text = message.get("text", "")
                            thread_tag = "[thread] " if message.get("is_thread") else ""

                            f.write(f"{thread_tag}[{timestamp}] {author_name}: {text}\n")

                        except KeyError as e:
                            print(f"Ошибка при обработке сообщения: {e}")

                print(f"История для {folder_name} сохранена успешно!")

            else:
                print(f"Нет сообщений для {folder_name}")

            print(f"Завершен бэкап сообщений для канала {folder_name}\n")

    except Exception as e:
        print(f"Произошла ошибка при выполнении бэкапа: {e}")


# Запуск основной программы
if __name__ == "__main__":
    main()
