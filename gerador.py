import json
import time
import random
from datetime import datetime
import os
CAMINHO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "eventos.log")

eventos = ["clique", "carrinho", "entrega"]
produtos = ["notebook", "celular", "teclado", "mouse"]

print("Gerador iniciado! Pressione Ctrl+C no terminal para parar.")

with open(CAMINHO, "a") as arquivo:
    while True: 
        dado = {
            "id_usuario": random.randint(1, 100),
            "evento": random.choice(eventos),
            "produto": random.choice(produtos),
            "timestamp": datetime.now().isoformat()
        }
        
        linha_json = json.dumps(dado)
        
        arquivo.write(linha_json + "\n")
        arquivo.flush() 
        
        print(f"Gerado: {linha_json}")
        
        time.sleep(2)