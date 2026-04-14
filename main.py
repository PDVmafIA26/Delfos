import discord
from discord.ext import commands

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f"Bot conectado como {bot.user}")
    
    canal_id = id  # ← ID del canal aquí
    canal = bot.get_channel(canal_id)
    
    if canal:
        await canal.send("¡Hola! Estoy online 🟢")

bot.run("token")