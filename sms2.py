import requests

# Endpoint oficial da API de nuvem do Traccar SMS Gateway
URL_CLOUD = "https://www.traccar.org/sms/"

# Cole aqui o token longo exibido no separador/opção "Cloud Service" da app
CLOUD_TOKEN = "fbutFSxNRUiBik70R49iPm:APA91bGYHtxERGmY0H2dcgzfjX9NTU0wqtXQ5azJArRClVj3S-bzJmQeoEkV0RCrll10zCnslxRqzAYCF78kvGwIiY84ya_YWivK9pCY2Wxwutu3Jl_-5Qo"

headers = {
    "Authorization": CLOUD_TOKEN,
    "Content-Type": "application/json"
}

payload = {
    "to": "+5534996390912",
    "message": "Teste enviado via Traccar Cloud Service!"
}

try:
    response = requests.post(URL_CLOUD, json=payload, headers=headers, timeout=10)
    print(f"Status Code: {response.status_code}")
    print(f"Resposta: {response.text}")
    
    if response.status_code in [200, 202]:
        print("Mensagem aceite pela nuvem e encaminhada para o telemóvel!")
    else:
        print("Ocorreu um erro no processamento do envio.")
except Exception as e:
    print(f"Erro na ligação ao servidor Traccar: {e}")