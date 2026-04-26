import discord
import json
import os
from config import BOT_TOKEN_DISCORD, CHANNEL_ID_DISCORD

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

    embed = discord.Embed(
        title       = info.get("title", "Sin título"),
        description = info.get("summary", ""),
    )
    embed.add_field(name="📋 Detalles", value=info.get("details", "N/A"), inline=False)
    embed.set_footer(text=f"Categoría: {topic.capitalize()}")

    return embed, info.get("image")

# ── Eventos ───────────────────────────────────────────
@bot.event
async def on_ready():
    print(f"✅ Bot conectado como {bot.user}")

    channel = bot.get_channel(CHANNEL_ID_DISCORD)
    if not channel:
        print(f"❌ Canal {CHANNEL_ID_DISCORD} no encontrado")
        return

    data = leer_json("example-request-body.json")
    embed, imagen_path = construir_embed(data)

    if imagen_path and os.path.exists(imagen_path):
        archivo = discord.File(imagen_path, filename="image.jpg")
        embed.set_image(url="attachment://image.jpg")
        await channel.send(embed=embed, file=archivo)
    else:
        await channel.send(embed=embed)

bot.run(BOT_TOKEN_DISCORD)