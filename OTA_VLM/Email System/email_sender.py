from pathlib import Path
import smtplib
from email.message import EmailMessage


def _load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.is_file():
        return
    for line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def _mask(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 4:
        return "*" * len(value)
    return value[:2] + "*" * (len(value) - 4) + value[-2:]


class EmailSender:
    def __init__(self, host, port, user, password, sender, recipient):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.sender = sender
        self.recipient = recipient

    @classmethod
    def from_env(cls):
        dotenv_path = Path(__file__).resolve().parent / ".env"
        _load_dotenv(dotenv_path)

        host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        port = int(os.getenv("SMTP_PORT", "587"))
        user = os.getenv("SMTP_USER")
        password = os.getenv("SMTP_PASS")
        sender = os.getenv("SMTP_FROM") or user
        recipient = os.getenv("SMTP_TO") or user
        debug = os.getenv("SMTP_DEBUG", "").lower() in {"1", "true", "yes", "y"}

        if debug:
            print(
                "[SMTP_DEBUG] host="
                + str(host)
                + " port="
                + str(port)
                + " user="
                + _mask(user or "")
                + " from="
                + _mask(sender or "")
                + " to="
                + _mask(recipient or "")
                + " pass="
                + _mask(password or "")
            )

        if not user or not password or not sender or not recipient:
            raise RuntimeError(
                "SMTP_USER, SMTP_PASS, SMTP_FROM, SMTP_TO must be set for --send-email."
            )

        return cls(host, port, user, password, sender, recipient)

    def send(self, subject, body):
        msg = EmailMessage()
        msg["From"] = self.sender
        msg["To"] = self.recipient
        msg["Subject"] = subject or "OTA Update Notice"
        msg.set_content(body or "")

        if self.port == 465:
            with smtplib.SMTP_SSL(self.host, self.port, timeout=20) as server:
                server.login(self.user, self.password)
                server.send_message(msg)
            return

        with smtplib.SMTP(self.host, self.port, timeout=20) as server:
            server.starttls()
            server.login(self.user, self.password)
            server.send_message(msg)
