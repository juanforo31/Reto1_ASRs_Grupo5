import json
import uuid
import random
import boto3
from datetime import datetime
import time
import os
from concurrent.futures import ThreadPoolExecutor


sqs = boto3.client('sqs', region_name='us-east-2')
QUEUE_URL = os.getenv("QUEUE_URL", "https://sqs.us-east-2.amazonaws.com/340234701815/NotificationQueue.fifo")



def simular_eventos(minutos=1, eventos_por_minuto=6500, segundos=60):
    tipos_evento = ["ORDER_ACCEPTED_prueba", "ORDER_CANCELLED_prueba", "TRADE_CORRECT_prueba", "CLOSE_PRINT_prueba"]
    resultado = []
    
    for _ in range(minutos):
        eventos_segundos = [[] for _ in range(segundos)]
        segundos_random = [int(random.expovariate(1 / (segundos / 2)) % segundos) for _ in range(eventos_por_minuto)]
        eventos_random = random.choices(tipos_evento, k=eventos_por_minuto)
        #segundos_random = random.choices(range(segundos), k=eventos_por_minuto)
        #eventos_random = random.choices(tipos_evento, k=eventos_por_minuto)

        for segundo, evento in zip(segundos_random, eventos_random):
            eventos_segundos[segundo].append(evento)

        resultado.append(eventos_segundos)

    return resultado

def send_message_to_sqs(body: dict, group_id: str):
    """Envía UN mensaje a cola SQS FIFO."""
    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(body),
        MessageGroupId=str(group_id),
        MessageDeduplicationId=str(body["id"])
    )
    return response

def send_messages_batch(messages: list[dict], group_id: str):
    """Envío en lotes (máx 10 por llamada) para mejorar performance."""
    results = {"Successful": 0, "Failed": []}
    for i in range(0, len(messages), 10):
        chunk = messages[i:i+10]
        entries = [
            {
                "Id": str(uuid.uuid4()),
                "MessageBody": json.dumps(m),
                "MessageGroupId": str(group_id),
                "MessageDeduplicationId": str(m["id"])
            }
            for m in chunk
        ]
        resp = sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=entries)
        results["Successful"] += len(resp.get("Successful", []))
        results["Failed"].extend(resp.get("Failed", []))
    return results

def send_messages_in_parallel(messages: list[dict], group_id: str):
    """Send messages in parallel using ThreadPoolExecutor."""
    results = {"Successful": 0, "Failed": []}
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = [
            executor.submit(send_messages_batch, messages[i:i+50], group_id)
            for i in range(0, len(messages), 50)
        ]
        for future in futures:
            batch_result = future.result()
            results["Successful"] += batch_result["Successful"]
            results["Failed"].extend(batch_result["Failed"])
    return results


def lambda_handler(event, context):
    if isinstance(event, dict) and "body" in event and isinstance(event["body"], str):
        try:
            event = json.loads(event["body"])
        except json.JSONDecodeError:
            pass

    if event.get("simulate"):
        minutos = int(event.get("minutos", 1))
        evxmin = int(event.get("eventos_por_minuto", 6500))
        segundos = int(event.get("segundos", 60))
        group_id = event.get("groupId", "simulator")

        simulacion = simular_eventos(minutos=minutos, eventos_por_minuto=evxmin, segundos=segundos)

        mensajes = []
        for minuto_idx, minuto in enumerate(simulacion, start=1):
            for segundo_idx, eventos in enumerate(minuto, start=1):
                if not eventos:
                    continue
                mensajes.append({
                    "id": f"{minuto_idx:02d}-{segundo_idx:02d}-{uuid.uuid4()}",
                    "type": "SIM_SECOND_AGGREGATE",
                    "generatedAt": datetime.utcnow().isoformat(timespec='milliseconds') + "Z",
                    "minute": minuto_idx,
                    "second": segundo_idx,
                    "count": len(eventos),
                    "events": eventos
                })

        batch_result = send_messages_in_parallel(mensajes, group_id)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "mode": "simulation",
                "minutes": minutos,
                "events_per_minute": evxmin,
                "seconds": segundos,
                "groupId": group_id,
                "messages_sent": batch_result["Successful"],
                "failed": batch_result["Failed"]
            }),
            "headers": {"Content-Type": "application/json"}
        }

    task = {
        "id": datetime.utcnow().isoformat(timespec='milliseconds') + "Z",
        "description": event['description'],
        "targetDate": event['targetDate'],
        "isCompleted": False
    }
    group_id = event.get('groupId', 'notifications')
    resp = send_message_to_sqs(task, group_id)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "mode": "task",
            "task": task,
            "messageId": resp["MessageId"]
        }),
        "headers": {"Content-Type": "application/json"}
}