import os
import json
import toml
import logging
import asyncio
import re
from datetime import datetime
from telethon import TelegramClient, events, errors, functions
from telethon.sessions import StringSession

# --- Dependency Check ---
try:
    import toml
except ImportError:
    print("The 'toml' library is not installed. Please install it with: pip install toml")
    exit()

# --- Configuration and Constants ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ASSETS_DIR = os.path.join(BASE_DIR, "assets1")
BACKEND_DIR = os.path.join(BASE_DIR, "backend_data")
CONFIG_FILE = os.path.join(ASSETS_DIR, "config1.toml")
FOLDER_FILE = os.path.join(BACKEND_DIR, "folders.json")
TASKS_FILE = os.path.join(BACKEND_DIR, "tasks.json")
LINKS_FILE = os.path.join(ASSETS_DIR, "links.txt")

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S"
)
LOGGER = logging.getLogger(__name__)

# --- Global Dictionaries and Sets ---
TASKS = {}
TASK_LOCK = asyncio.Lock()

# --- Commands ---
COMMAND_PREFIX = '.'
COMMAND_LIST = f"{COMMAND_PREFIX}lists"
COMMAND_ADD = f"{COMMAND_PREFIX}add"
COMMAND_START = f"{COMMAND_PREFIX}start"
COMMAND_STOP = f"{COMMAND_PREFIX}stop"
COMMAND_CMD = f"{COMMAND_PREFIX}cmd"
COMMAND_LINKS = f"{COMMAND_PREFIX}links"
COMMAND_TASKS = f"{COMMAND_PREFIX}tasks"
COMMAND_PING = f"{COMMAND_PREFIX}ping"

NOTIFY_CHANNEL = "@AdsBotLog"

class TelegramBot:
    def __init__(self):
        self.config = {}
        self.folders = {}
        self.client = None
        self._setup_files_and_directories()
        self._load_all_data()

    def _setup_files_and_directories(self):
        """Ensures necessary directories and files exist."""
        os.makedirs(ASSETS_DIR, exist_ok=True)
        os.makedirs(BACKEND_DIR, exist_ok=True)
        
        # Create a new, clean config file if it doesn't exist
        if not os.path.exists(CONFIG_FILE):
            default_config = {
                "telegram": {
                    "phone_number": "+923023495029",
                    "api_id": 24852819,
                    "api_hash": "26eb9e111fb377d296abb6a548cd3387",
                    "monitor_chat": "@NoBwo",
                    "session": ""
                },
                "sending": {
                    "send_interval": 60,
                    "loop_interval": 3600
                }
            }
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                toml.dump(default_config, f)
            LOGGER.info(f"Created new file: {CONFIG_FILE}")

        # Create other data files if they don't exist
        for path in [FOLDER_FILE, TASKS_FILE, LINKS_FILE]:
            if not os.path.exists(path):
                with open(path, "w", encoding="utf-8") as f:
                    if path.endswith(".json"):
                        json.dump({} if path == FOLDER_FILE else [], f)
                    else:
                        f.write("")
                LOGGER.info(f"Created new file: {path}")

    def _load_all_data(self):
        """Loads all configuration and data files at once."""
        try:
            with open(CONFIG_FILE, "r") as f:
                self.config = toml.load(f)
            self.phone_number = self.config["telegram"]["phone_number"]
            self.api_id = self.config["telegram"]["api_id"]
            self.api_hash = self.config["telegram"]["api_hash"]
            self.monitor_chat = self.config["telegram"]["monitor_chat"]
            self.session_string = self.config["telegram"]["session"]
            self.loop_interval = self.config["sending"].get("loop_interval", 3600)
            self.send_interval = self.config["sending"].get("send_interval", 60)

            with open(FOLDER_FILE, "r") as f:
                self.folders = json.load(f)
        except (IOError, toml.TomlDecodeError, json.JSONDecodeError, KeyError) as e:
            LOGGER.critical(f"Failed to load data files or config. Check file integrity. Error: {e}")
            raise

    def _save_folders(self):
        """Saves the current folders dictionary to file."""
        try:
            with open(FOLDER_FILE, "w", encoding="utf-8") as f:
                json.dump(self.folders, f, indent=4)
        except IOError as e:
            LOGGER.error(f"Failed to save folders to file: {e}")

    async def _save_tasks(self):
        """Saves active task IDs and their last run timestamps to a file."""
        try:
            task_data = []
            async with TASK_LOCK:
                for tid, data in TASKS.items():
                    task_data.append({
                        "folder": tid[0],
                        "message_id": tid[1],
                        "chat_id": tid[2],
                        "last_run_timestamp": data["last_run_timestamp"].isoformat()
                    })
            with open(TASKS_FILE, "w", encoding="utf-8") as f:
                json.dump(task_data, f, indent=4)
        except IOError as e:
            LOGGER.error(f"Failed to save tasks to file: {e}")

    def _save_session(self):
        """Saves the session string to the TOML config file."""
        try:
            session_string = self.client.session.save()
            self.config["telegram"]["session"] = session_string
            with open(CONFIG_FILE, "w") as f:
                toml.dump(self.config, f)
            LOGGER.info("Session string saved successfully.")
        except Exception as e:
            LOGGER.error(f"Failed to save session string: {e}")

    async def connect(self):
        """Connects to Telegram, handling authentication."""
        self.client = TelegramClient(
            StringSession(self.session_string),
            self.api_id,
            self.api_hash
        )
        await self.client.connect()
        if not await self.client.is_user_authorized():
            LOGGER.info("User not authorized. Starting authentication process.")
            try:
                await self.client.start(phone=self.phone_number)
            except errors.SessionPasswordNeededError:
                password = input("Enter your 2FA password: ")
                await self.client.start(phone=self.phone_number, password=password)
            except Exception as e:
                LOGGER.critical(f"Authentication failed: {e}")
                await self.client.disconnect()
                raise
        self._save_session()
        LOGGER.info("Bot connected and authorized.")

    async def start(self):
        """Starts the bot and sets up event handlers."""
        await self.connect()
        
        try:
            connected_text = (
                "🤖 *Bot Connected Successfully!* \n\n"
                f"🕒 `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n"
                "✅ All systems operational.\n\n"
                f"Use `{COMMAND_CMD}` to see available commands."
            )
            await self.client.send_message('me', connected_text, parse_mode='md')
        except Exception as e:
            LOGGER.error(f"Welcome message failed: {e}")

        await self._resume_tasks()

        @self.client.on(events.NewMessage(chats=self.monitor_chat))
        async def handler(event):
            text = (getattr(event, "raw_text", "") or "").strip()
            if not text.startswith(COMMAND_PREFIX):
                return
            
            cmd = text.split()[0]
            
            if cmd == COMMAND_LIST:
                await self.list_folders(event)
            elif cmd.startswith(COMMAND_ADD):
                await self.add_groups(event)
            elif cmd.startswith(COMMAND_START):
                await self.start_forwarding(event)
            elif cmd == COMMAND_TASKS:
                await self.list_tasks(event)
            elif cmd.startswith(COMMAND_STOP):
                await self.stop_task(event)
            elif cmd == COMMAND_CMD:
                await self.show_commands(event)
            elif cmd == COMMAND_LINKS:
                await self.extract_group_links(event)
            elif cmd == COMMAND_PING:
                await self.ping_command(event)
            else:
                await event.reply(f"❌ Invalid command. Use `{COMMAND_CMD}` to see all commands.")

        await self.client.run_until_disconnected()

    # --- Command Handlers ---
    async def show_commands(self, event):
        """Sends a list of available commands to the user."""
        commands = (
            "📌 *Available commands*\n\n"
            f"• `{COMMAND_LIST}` — show folders\n"
            f"• `{COMMAND_ADD} folder_name group_link[:topic_id]` — add group(s) to folder\n"
            f"• `{COMMAND_START} folder_name` — start hourly forwarding (reply to a message)\n"
            f"• `{COMMAND_TASKS}` — list active tasks\n"
            f"• `{COMMAND_STOP} task_id` — stop a task\n"
            f"• `{COMMAND_PING}` — check if the bot is online\n"
            f"• `{COMMAND_CMD}` — show this command list\n"
            f"• `{COMMAND_LINKS}` — extract public group links and save to `links.txt`\n\n"
        )
        await event.reply(commands, parse_mode='md')

    async def ping_command(self, event):
        """A simple ping command to check bot status."""
        await event.reply("Pong! 🏓 I'm online and ready.")

    async def extract_group_links(self, event):
        """Extract public group usernames/links and save to assets1/links.txt."""
        try:
            dialogs = await self.client.get_dialogs()
            links_set = set()
            for dlg in dialogs:
                if getattr(dlg, "is_group", False):
                    ent = dlg.entity
                    username = getattr(ent, "username", None)
                    if username:
                        links_set.add(f"https://t.me/{username}")
            
            links_list = sorted(list(links_set))
            if not links_list:
                await event.reply("⚠ No public group links found.")
                return
            
            with open(LINKS_FILE, "w", encoding="utf-8") as f:
                f.write("\n".join(links_list))
            
            await self.client.send_file(
                event.chat_id,
                LINKS_FILE,
                caption="📁 Your public group links",
                force_document=True
            )
            LOGGER.info(f"Extracted {len(links_list)} group links.")
        except Exception as e:
            LOGGER.error(f"extract_group_links error: {e}")
            await event.reply(f"❌ Error: {e}")

    async def add_groups(self, event):
        """Adds group links to a folder."""
        cmd = (getattr(event, "raw_text", "") or "").split()
        if len(cmd) < 3:
            await event.reply(f"Usage: `{COMMAND_ADD} folder_name group_link[:topic_id]`")
            return
        
        folder = cmd[1]
        links = cmd[2:]
        normalized = self._normalize_links(links)
        
        if folder not in self.folders:
            self.folders[folder] = []
        
        added = 0
        for ln in normalized:
            if ln not in self.folders[folder]:
                self.folders[folder].append(ln)
                added += 1
        
        self._save_folders()
        await event.reply(f"✅ Added {added} new item(s) to `{folder}`.")
        LOGGER.info(f"Added {added} links to folder '{folder}'.")

    async def list_folders(self, event):
        """Lists all existing folders and their item counts."""
        if not self.folders:
            await event.reply("⚠ No folders found.")
        else:
            lines = [f"📁 *{name}* — `{len(lst)}` items" for name, lst in self.folders.items()]
            text = "📂 *Folders*\n\n" + "\n".join(lines)
            await event.reply(text, parse_mode='md')

    async def start_forwarding(self, event):
        """Starts a new forwarding task."""
        cmd = (getattr(event, "raw_text", "") or "").split()
        if len(cmd) < 2 or not event.is_reply:
            await event.reply(f"⚠ Reply to a message with `{COMMAND_START} folder_name`")
            return
        
        folder = cmd[1]
        if folder not in self.folders or not self.folders[folder]:
            await event.reply(f"⚠ Folder `{folder}` not found or is empty.")
            return
        
        msg = await event.get_reply_message()
        if not msg:
            await event.reply("❌ Cannot get the replied message.")
            return

        chat_id = msg.chat_id
        task_id = (folder, msg.id, chat_id)
        
        async with TASK_LOCK:
            if task_id in TASKS:
                await event.reply(f"⚠ Task `{task_id[0]}_{task_id[1]}` already running.")
                return
            
            TASKS[task_id] = {
                "task": asyncio.create_task(self._auto_forward(folder, msg.id, chat_id)),
                "last_run_timestamp": datetime.now()
            }
            await self._save_tasks()
            
            start_message = (
                f"🚀 *Hourly Task Started!* (ID: `{task_id[0]}_{task_id[1]}`)"
            )
            await event.reply(start_message, parse_mode='md')
            LOGGER.info(f"Started new task: '{task_id}'.")

    async def list_tasks(self, event):
        """Lists all active tasks."""
        if not TASKS:
            await event.reply("⚠ No active tasks.")
            return
        task_list = [f"• `{tid[0]}_{tid[1]}`" for tid in TASKS.keys()]
        await event.reply("📋 *Active Tasks:*\n" + "\n".join(task_list), parse_mode='md')

    async def stop_task(self, event):
        """Stops a specific task by its ID."""
        cmd = (getattr(event, "raw_text", "") or "").split()
        if len(cmd) < 2:
            await event.reply(f"Usage: `{COMMAND_STOP} task_id`")
            return
        
        tid_str = cmd[1]
        try:
            folder_name, msg_id = tid_str.split('_')
            msg_id = int(msg_id)
        except ValueError:
            await event.reply("❌ Invalid task ID format. Use `folder_name_message_id`.")
            return

        async with TASK_LOCK:
            tid_found = None
            for tid in TASKS.keys():
                if tid[0] == folder_name and tid[1] == msg_id:
                    tid_found = tid
                    break

            if tid_found:
                task = TASKS.pop(tid_found)["task"]
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                await self._save_tasks()
                await event.reply(f"⏹ Stopped task `{tid_str}`.")
                LOGGER.info(f"Task '{tid_str}' stopped.")
            else:
                await event.reply("⚠ Task not found.")

    # --- Internal Helper Methods ---
    def _normalize_links(self, links):
        """Normalizes various link formats to a consistent t.me URL."""
        normalized = []
        for l in links:
            l = l.strip()
            if l.startswith("@"):
                normalized.append(f"https://t.me/{l.lstrip('@')}")
            else:
                normalized.append(l)
        return normalized

    def _parse_group_topic(self, link):
        """Parses a link to extract the group identifier and topic ID."""
        m_topic = re.match(r"https?://t\.me/([^/]+)/(\d+)", link)
        if m_topic:
            return m_topic.group(1), int(m_topic.group(2))

        m_private = re.match(r"https?://t\.me/c/(\d+)", link)
        if m_private:
            return int(f"-100{m_private.group(1)}"), None

        m_public = re.match(r"https?://t\.me/([^/]+)", link)
        if m_public:
            return m_public.group(1), None

        return link, None

    async def _get_group_link(self, ent):
        """Returns a clickable link to a group/channel based on its entity."""
        username = getattr(ent, "username", None)
        if username:
            return f"https://t.me/{username}"
        
        ent_id = getattr(ent, "id", None)
        if ent_id:
            return f"https://t.me/c/{str(ent_id).lstrip('-100')}"
        
        return None

    async def _perform_forward_safe(self, entry, message):
        """Performs a single message forward with robust error handling."""
        grp, topic = self._parse_group_topic(entry)
        
        try:
            target = grp
            if isinstance(target, str) and target.lstrip('-').isdigit():
                target = int(target)
            
            ent = await self.client.get_entity(target)
            
            if topic:
                await self.client(functions.messages.ForwardMessagesRequest(
                    from_peer=message.chat_id,
                    id=[message.id],
                    to_peer=ent,
                    top_msg_id=topic
                ))
            else:
                await self.client.forward_messages(ent, message.id, message.chat_id)
            
            group_link = await self._get_group_link(ent)
            return group_link, True
        
        except errors.rpcerrorlist.FloodWaitError as e:
            wait_time = e.seconds + 5
            LOGGER.warning(f"Hit FloodWait. Waiting for {wait_time} seconds before retrying.")
            return None, f"❌ FloodWaitError. Script will automatically pause for {wait_time} seconds. Error: {e}"
        except (
            errors.rpcerrorlist.PeerIdInvalidError,
            ValueError
        ) as e:
            return None, f"❌ Failed: Invalid group or channel link. Error: {e}"
        except errors.rpcerrorlist.ChannelPrivateError:
            return None, "❌ Failed: This is a private channel."
        except errors.rpcerrorlist.ChatWriteForbiddenError:
            return None, "❌ Failed: You don't have permission to post in this group."
        except errors.rpcerrorlist.UserBannedInChannelError:
            return None, "❌ Failed: You have been banned from this group."
        except Exception as e:
            return None, f"❌ An unexpected error occurred: {repr(e)}"

    async def _forward_message(self, folder_name, message):
        """Forwards a single message to all groups in a specified folder."""
        if folder_name not in self.folders:
            LOGGER.warning(f"Folder not found: {folder_name}")
            return
        
        for entry in self.folders[folder_name]:
            group_link, status = await self._perform_forward_safe(entry, message)
            
            # If FloodWaitError is returned, we need to wait and then re-attempt
            if isinstance(status, str) and "FloodWaitError" in status:
                match = re.search(r'pause for (\d+) seconds', status)
                wait_time = int(match.group(1)) if match else 60
                await self.client.send_message(NOTIFY_CHANNEL, status, link_preview=False)
                await asyncio.sleep(wait_time)
                # Re-attempt the forward after waiting
                group_link, status = await self._perform_forward_safe(entry, message)
                
            if status is True:
                text = f"📢 Your post has been shared successfully.\n📎 Group Link: {group_link or entry}"
                LOGGER.info(f"Forwarded message to '{entry}'.")
            else:
                text = f"❌ Failed to share post to `{entry}`.\nError: {status}"
                LOGGER.error(f"Forwarding to '{entry}' failed with error: {status}")
                
            await self.client.send_message(NOTIFY_CHANNEL, text, link_preview=False)
            await asyncio.sleep(self.send_interval)

    async def _auto_forward(self, folder_name, message_id, chat_id):
        """The main auto-forwarding loop."""
        try:
            while True:
                try:
                    message = await self.client.get_messages(chat_id, ids=message_id)
                    if not message:
                        LOGGER.error(f"Message ID {message_id} not found in chat {chat_id}. Cancelling task.")
                        raise asyncio.CancelledError
                except Exception as e:
                    LOGGER.error(f"Could not retrieve message {message_id} from chat {chat_id}: {e}")
                    raise asyncio.CancelledError

                await self._forward_message(folder_name, message)

                timestamp = datetime.now()
                async with TASK_LOCK:
                    if (folder_name, message_id, chat_id) in TASKS:
                        TASKS[(folder_name, message_id, chat_id)]["last_run_timestamp"] = timestamp
                await self._save_tasks()

                await self.client.send_message(
                    NOTIFY_CHANNEL,
                    f"✅ *Round Complete!* (Task: `{folder_name}_{message_id}`)\n\n"
                    f"🕒 Time: `{timestamp.strftime('%Y-%m-%d %H:%M:%S')}`\n\n"
                    f"Waiting {int(self.loop_interval / 3600)} hour(s) until the next round begins.",
                    parse_mode='md'
                )
                
                LOGGER.info(f"Round complete for task {folder_name}_{message_id}. Waiting for {self.loop_interval} seconds.")
                await asyncio.sleep(self.loop_interval)
        
        except asyncio.CancelledError:
            LOGGER.info(f"auto_forward for {folder_name} (msg_id: {message_id}) cancelled.")
        except Exception as e:
            LOGGER.error(f"Task for {folder_name} failed unexpectedly: {e}", exc_info=True)
            await self.client.send_message(
                NOTIFY_CHANNEL,
                f"❌ Task for folder `{folder_name}` failed with error: `{repr(e)}`",
                link_preview=False
            )

    async def _resume_tasks(self):
        """Loads and resumes tasks from the tasks.json file, respecting cooldown."""
        try:
            with open(TASKS_FILE, "r") as f:
                saved_tasks = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            LOGGER.warning("No tasks to resume or tasks file is empty/corrupt.")
            return

        if not saved_tasks:
            return
        
        LOGGER.info(f"Attempting to resume {len(saved_tasks)} tasks...")
        for task_data in saved_tasks:
            folder_name = task_data.get("folder")
            message_id = task_data.get("message_id")
            chat_id = task_data.get("chat_id")
            last_run_timestamp_str = task_data.get("last_run_timestamp")
            
            if folder_name and message_id and chat_id and last_run_timestamp_str:
                task_id = (folder_name, message_id, chat_id)
                if task_id not in TASKS:
                    try:
                        last_run_timestamp = datetime.fromisoformat(last_run_timestamp_str)
                        time_since_last_run = (datetime.now() - last_run_timestamp).total_seconds()
                        
                        remaining_time = max(0, self.loop_interval - time_since_last_run)

                        if remaining_time > 0:
                            LOGGER.info(f"Task {task_id} will resume in {int(remaining_time)} seconds.")
                            await self.client.send_message(
                                NOTIFY_CHANNEL,
                                f"⏳ Resuming task (ID: `{folder_name}_{message_id}`) in `{int(remaining_time / 60)}` minutes.",
                                link_preview=False,
                                parse_mode='md'
                            )
                            await asyncio.sleep(remaining_time)
                        
                        TASKS[task_id] = {
                            "task": asyncio.create_task(self._auto_forward(folder_name, message_id, chat_id)),
                            "last_run_timestamp": datetime.now()
                        }
                        
                        await self.client.send_message(
                            NOTIFY_CHANNEL,
                            f"🔄 Resumed task (ID: `{folder_name}_{message_id}`) successfully.",
                            link_preview=False,
                            parse_mode='md'
                        )
                        LOGGER.info(f"Resumed task: {task_id}")
                    
                    except Exception as e:
                        LOGGER.error(f"Failed to resume task {task_id}: {e}")
                        await self.client.send_message(
                            NOTIFY_CHANNEL,
                            f"❌ Failed to resume task (ID: `{folder_name}_{message_id}`): `{e}`",
                            link_preview=False,
                            parse_mode='md'
                        )
        
        await self._save_tasks()

if __name__ == "__main__":
    bot = TelegramBot()
    try:
        asyncio.run(bot.start())
    except Exception as main_e:
        LOGGER.critical(f"Bot failed to start: {main_e}", exc_info=True)