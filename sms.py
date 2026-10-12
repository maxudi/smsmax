import requests

# URL exibida na tela do Traccar SMS Gateway no celular
url = "http://100.87.17.40:8082/"

# Se o app gerou um Token/API Key na tela, coloque aqui (se não houver, pode deixar vazio)
token = "2c6b6533-a65f-4070-a829-8b6f5475ab1b"

headers = {
    "Content-Type": "application/json"
}

if token:
    headers["Authorization"] = token

payload = {
    "to": "+5534996390912",
    "message": "Alerta de sistema22222"
}

try:
    response = requests.post(url, json=payload, headers=headers, timeout=5)
    print(f"Status Code: {response.status_code}")
    print(f"Resposta: {response.text}")
except Exception as e:
    print(f"Erro ao conectar com o celular: {e}")