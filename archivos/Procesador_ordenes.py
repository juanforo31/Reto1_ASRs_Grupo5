import json
import uuid
from datetime import datetime

print("Loading function")

# --- Simulación: 5 clientes premium en memoria ---
CLIENTS = [
    {"customerId": "u-001", "name": "María Gómez1",  "plan": "premium", "channel": "email", "address": "maria@example.com"},
    {"customerId": "u-002", "name": "Juan Pérez2",   "plan": "premium", "channel": "sms",   "phone": "‪+57 3001112222‬"},
    {"customerId": "u-003", "name": "Ana Torres3",   "plan": "premium", "channel": "push",  "deviceId": "dev-003"},
    {"customerId": "u-004", "name": "Carlos Ruiz4",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-005", "name": "Luisa Medina5", "plan": "premium", "channel": "sms",   "phone": "‪+57 3013334444‬"},
    {"customerId": "u-001", "name": "María Gómez6",  "plan": "premium", "channel": "email", "address": "maria@example.com"},
    {"customerId": "u-002", "name": "Juan Pérez7",   "plan": "premium", "channel": "sms",   "phone": "‪+57 3001112222‬"},
    {"customerId": "u-003", "name": "Ana Torres8",   "plan": "premium", "channel": "push",  "deviceId": "dev-003"},
    {"customerId": "u-004", "name": "Carlos Ruiz9",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-005", "name": "Luisa Medina10", "plan": "premium", "channel": "sms",   "phone": "‪+57 3013334444‬"},
    {"customerId": "u-001", "name": "María Gómez11",  "plan": "premium", "channel": "email", "address": "maria@example.com"},
    {"customerId": "u-002", "name": "Juan Pérez12",   "plan": "premium", "channel": "sms",   "phone": "‪+57 3001112222‬"},
    {"customerId": "u-003", "name": "Ana Torres13",   "plan": "premium", "channel": "push",  "deviceId": "dev-003"},
    {"customerId": "u-004", "name": "Carlos Ruiz14",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-005", "name": "Luisa Medina15", "plan": "premium", "channel": "sms",   "phone": "‪+57 3013334444‬"},
    {"customerId": "u-004", "name": "Carlos Ruiz16",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-005", "name": "Luisa Medina17", "plan": "premium", "channel": "sms",   "phone": "‪+57 3013334444‬"},
    {"customerId": "u-004", "name": "Carlos Ruiz18",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-005", "name": "Luisa Medina19", "plan": "premium", "channel": "sms",   "phone": "‪+57 3013334444‬"},
    {"customerId": "u-005", "name": "Luisa Medina20", "plan": "premium", "channel": "sms",   "phone": "‪+57 3013334444‬"},
    {"customerId": "u-005", "name": "Luisa Medina21", "plan": "premium", "channel": "sms",   "phone": "‪+57 3013334444‬"},
    {"customerId": "u-005", "name": "Luisa Medina22", "plan": "premium", "channel": "sms",   "phone": "‪+57 3013334444‬"},
    {"customerId": "u-005", "name": "Luisa Medina23", "plan": "premium", "channel": "sms",   "phone": "‪+57 3013334444‬"},
    {"customerId": "u-005", "name": "Luisa Medina24", "plan": "premium", "channel": "sms",   "phone": "‪+57 3013334444‬"},
    {"customerId": "u-005", "name": "Luisa Medina25", "plan": "premium", "channel": "sms",   "phone": "‪+57 3013334444‬"},
    {"customerId": "u-005", "name": "Luisa Medina26", "plan": "premium", "channel": "sms",   "phone": "‪+57 3013334444‬"},
    {"customerId": "u-005", "name": "Luisa Medina27", "plan": "premium", "channel": "sms",   "phone": "‪+57 3013334444‬"},
    {"customerId": "u-004", "name": "Carlos Ruiz28",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-005", "name": "Luisa Medina29", "plan": "premium", "channel": "sms",   "phone": "‪+57 3013334444‬"},
    {"customerId": "u-004", "name": "Carlos Ruiz30",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-005", "name": "Luisa Medina31", "plan": "premium", "channel": "sms",   "phone": "‪+57 3013334444‬"},
    {"customerId": "u-004", "name": "Carlos Ruiz132",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-004", "name": "Carlos Ruiz33",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-005", "name": "Luisa Medina34", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-004", "name": "Carlos Ruiz35",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-005", "name": "Luisa Medina36", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-004", "name": "Carlos Ruiz37",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-001", "name": "María Gómez1",  "plan": "premium", "channel": "email", "address": "maria@example.com"},
    {"customerId": "u-002", "name": "Juan Pérez2",   "plan": "premium", "channel": "sms",   "phone": "+57 3001112222"},
    {"customerId": "u-003", "name": "Ana Torres3",   "plan": "premium", "channel": "push",  "deviceId": "dev-003"},
    {"customerId": "u-004", "name": "Carlos Ruiz4",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-005", "name": "Luisa Medina5", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-001", "name": "María Gómez6",  "plan": "premium", "channel": "email", "address": "maria@example.com"},
    {"customerId": "u-002", "name": "Juan Pérez7",   "plan": "premium", "channel": "sms",   "phone": "+57 3001112222"},
    {"customerId": "u-003", "name": "Ana Torres8",   "plan": "premium", "channel": "push",  "deviceId": "dev-003"},
    {"customerId": "u-004", "name": "Carlos Ruiz9",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-005", "name": "Luisa Medina10", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-001", "name": "María Gómez11",  "plan": "premium", "channel": "email", "address": "maria@example.com"},
    {"customerId": "u-002", "name": "Juan Pérez12",   "plan": "premium", "channel": "sms",   "phone": "+57 3001112222"},
    {"customerId": "u-003", "name": "Ana Torres13",   "plan": "premium", "channel": "push",  "deviceId": "dev-003"},
    {"customerId": "u-004", "name": "Carlos Ruiz14",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-005", "name": "Luisa Medina15", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-004", "name": "Carlos Ruiz16",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-005", "name": "Luisa Medina17", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-004", "name": "Carlos Ruiz18",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-005", "name": "Luisa Medina19", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-005", "name": "Luisa Medina20", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-005", "name": "Luisa Medina21", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-005", "name": "Luisa Medina22", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-005", "name": "Luisa Medina23", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-005", "name": "Luisa Medina24", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-005", "name": "Luisa Medina25", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-005", "name": "Luisa Medina26", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-005", "name": "Luisa Medina27", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-004", "name": "Carlos Ruiz28",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-005", "name": "Luisa Medina29", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-004", "name": "Carlos Ruiz30",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-005", "name": "Luisa Medina31", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-004", "name": "Carlos Ruiz132",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-004", "name": "Carlos Ruiz33",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-005", "name": "Luisa Medina34", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-004", "name": "Carlos Ruiz35",  "plan": "premium", "channel": "email", "address": "carlos@example.com"},
    {"customerId": "u-005", "name": "Luisa Medina36", "plan": "premium", "channel": "sms",   "phone": "+57 3013334444"},
    {"customerId": "u-004", "name": "Carlos Ruiz37",  "plan": "premium", "channel": "email", "address": "carlos@example.com"}
]

def now_iso():
    return datetime.utcnow().isoformat(timespec="milliseconds") + "Z"

def make_notification(client: dict, payload: dict, message_id: str) -> dict:
    
    order_type = payload.get("order_type") or payload.get("type") or "EVENT"
    count = payload.get("count")  # por si llega de agregados
    base_msg = f"Hola {client.get('name','cliente')}, nueva orden {order_type}"
    if count is not None:
        base_msg += f" con {count} evento(s)"
    return {
        "id": f"notif-{uuid.uuid4()}",
        "createdAt": now_iso(),
        "customerId": client["customerId"],
        "plan": client["plan"],
        "channel": client.get("channel"),
        "message": base_msg + ".",
        "source": {
            "sqsMessageId": message_id,
            "payloadBrief": {
                "order_type": payload.get("events"),
                "type": payload.get("type"),
                "minute": payload.get("minute"),
                "second": payload.get("second"),
                "count": payload.get("count"),
            },
        },
    }

def lambda_handler(event, context):
    records = event.get("Records", [])
    total_notifications = 0

    for record in records:
        message_id = record.get("messageId")
        body_raw = record.get("body", "{}")
        try:
            payload = json.loads(body_raw)
        except json.JSONDecodeError:
            payload = {"raw": body_raw}


        for client in CLIENTS:
            notif = make_notification(client, payload, message_id)
            print(json.dumps(notif, ensure_ascii=False))  # simulación de envío
            total_notifications += 1

    return {
        "statusCode": 200,
        "body": json.dumps({
            "sqs_messages": len(records),
            "premium_clients": len(CLIENTS),
            "notifications_created": total_notifications
        }),
        "headers": {"Content-Type": "application/json"}
}