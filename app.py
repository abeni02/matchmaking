from flask import Flask, request
import telebot

app = Flask(__name__)
bot = telebot.TeleBot("YOUR_BOT_TOKEN")

@app.route('/webhook', methods=['POST'])
def webhook():
    update = telebot.types.Update.de_json(request.get_json())
    bot.process_new_updates([update])
    return 'OK', 200

@bot.message_handler(commands=['start'])
def handle_start(message):
    bot.reply_to(message, "Hello! I'm awake!")

if __name__ == '__main__':
    bot.remove_webhook()  # Clear any existing webhook
    bot.set_webhook(url='https://your-domain.com/webhook')  # Set webhook URL
    app.run(debug=True)
