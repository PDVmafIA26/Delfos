import requests

def audit_official_closed_positions(wallet_address):
    # Usamos el endpoint exacto que has descubierto en la OpenAPI
    url = "https://data-api.polymarket.com/closed-positions"
    
    # Parámetros según tu documentación
    params = {
        "user": wallet_address,
        "limit": 50,             # Podemos pedir hasta 50 resultados por página
        "sortBy": "REALIZEDPNL", # Ordenamos por el dinero ganado/perdido
        "sortDirection": "DESC"  # De mayor a menor ganancia
    }
    
    print(f"📚 Descargando historial oficial de la wallet: {wallet_address[:8]}...\n")
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        closed_positions = response.json()
        
        if not closed_positions:
            print("Esta wallet no tiene posiciones cerradas en el registro oficial.")
            return

        total_ganado = 0
        total_perdido = 0
        
        print(f"{'MERCADO':<50} | {'PREDICCIÓN':<10} | {'RESULTADO':<15} | {'CANTIDAD'}")
        print("-" * 95)
        
        # Iteramos sobre el array de ClosedPosition
        for pos in closed_positions:
            market_name = pos.get("title", "Desconocido")
            if len(market_name) > 47:
                market_name = market_name[:44] + "..."
                
            outcome = pos.get("outcome", "N/A")
            realized_pnl = float(pos.get("realizedPnl", 0))
            
            # Como el endpoint ya nos da posiciones cerradas, evaluamos el PnL directamente
            if realized_pnl > 0:
                estado = "✅ GANADO"
                cantidad = f"+${realized_pnl:,.2f}"
                total_ganado += realized_pnl
            elif realized_pnl < 0:
                estado = "❌ PERDIDO"
                cantidad = f"-${abs(realized_pnl):,.2f}"
                total_perdido += abs(realized_pnl)
            else:
                estado = "➖ EMPATE"
                cantidad = "$0.00"
                
            print(f"{market_name:<50} | {outcome:<10} | {estado:<15} | {cantidad}")

        print("-" * 95)
        print("📊 RESUMEN DE LA MUESTRA (Top 50 operaciones)")
        print("-" * 30)
        
        beneficio_neto = total_ganado - total_perdido
        
        if beneficio_neto > 0:
            print(f"🤑 BALANCE NETO DE LA MUESTRA: +${beneficio_neto:,.2f}")
        elif beneficio_neto < 0:
            print(f"📉 BALANCE NETO DE LA MUESTRA: -${abs(beneficio_neto):,.2f}")
        else:
            print("⚖️ BALANCE NETO: $0.00")

    except requests.exceptions.RequestException as e:
        print(f"❌ Error de conexión con la Data API: {e}")

if __name__ == "__main__":
    WALLET_ADDRESS = "0x56687bf447db6ffa42ffe2204a05edaa20f55839" 
    audit_official_closed_positions(WALLET_ADDRESS)