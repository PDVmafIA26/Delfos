import discord
import json
import os
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────
TOKEN     = os.getenv("DISCORD_TOKEN")
CANAL_ID  = int(os.getenv("DISCORD_CANAL_ID"))
JSON_PATH = os.getenv("JSON_PATH", "notification.json")

# ── Colores por topic ─────────────────────────────────
TOPIC_COLORES = {
    "politics": discord.Color.red(),
    "sports":   discord.Color.green(),
    "tech":     discord.Color.blue(),
}

# ── Cliente ───────────────────────────────────────────
intents = discord.Intents.default()
bot = discord.Client(intents=intents)

# ── Funciones ─────────────────────────────────────────
def leer_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def construir_embed(data: dict):
    topic = data.get("topic", "general")
    info  = data.get("data", {})
    color = TOPIC_COLORES.get(topic, discord.Color.greyple())

    embed = discord.Embed(
        title       = info.get("title", "Sin título"),
        description = info.get("summary", ""),
        color       = color
    )
    embed.add_field(name="📋 Detalles", value=info.get("details", "N/A"), inline=False)
    embed.set_footer(text=f"Categoría: {topic.capitalize()}")

    return embed, info.get("image")

# ── Eventos ───────────────────────────────────────────
@bot.event
async def on_ready():
    print(f"✅ Bot conectado como {bot.user}")

    canal = bot.get_channel(CANAL_ID)
    if not canal:
        print(f"❌ Canal {CANAL_ID} no encontrado")
        return

    datos = leer_json(JSON_PATH)
    embed, imagen_path = construir_embed(datos)

    if imagen_path and os.path.exists(imagen_path):
        archivo = discord.File(imagen_path, filename="image.jpg")
        embed.set_image(url="attachment://image.jpg")
        await canal.send(embed=embed, file=archivo)
    else:
        await canal.send(embed=embed)

bot.run(TOKEN)