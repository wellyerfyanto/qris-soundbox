import os
import json
import queue
from collections import defaultdict
from flask import Flask, render_template, request, Response, jsonify
from flask_cors import CORS

app = Flask(__name__)
# Mengizinkan koneksi dari Blogger / Domain lain
CORS(app)

# Menampung antrean transaksi berdasarkan Merchant ID (MID)
merchant_queues = defaultdict(queue.Queue)

def extract_universal_payment(data):
    """
    Ekstrak MID, Nominal, Status, dan Sumber Pembayaran 
    dari berbagai jenis format Webhook Payment Gateway.
    """
    mid = None
    is_success = False
    amount = 0
    source_app = "QRIS"

    # 1. Format Midtrans
    if 'transaction_status' in data or 'merchant_id' in data:
        mid = data.get('merchant_id') or data.get('store_id')
        status = data.get('transaction_status', '')
        is_success = status in ['settlement', 'capture']
        amount = int(float(data.get('gross_amount', 0)))
        source_app = data.get('payment_type') or data.get('issuer') or 'Midtrans QRIS'

    # 2. Format Xendit
    elif 'status' in data and ('business_id' in data or 'qr_code' in data):
        mid = data.get('business_id') or data.get('qr_code', {}).get('external_id')
        status = str(data.get('status', '')).upper()
        is_success = status in ['SUCCEEDED', 'COMPLETED', 'PAID', 'ACTIVE']
        amount = int(float(data.get('amount', 0)))
        source_app = data.get('channel_code') or data.get('payment_method') or 'Xendit QRIS'

    # 3. Format DOKU / Custom / Universal Fallback
    elif 'transaction' in data or 'order' in data:
        trans = data.get('transaction', {})
        order = data.get('order', {})
        mid = data.get('merchant', {}).get('id') or order.get('merchant_id')
        status = str(trans.get('status') or data.get('status', '')).upper()
        is_success = status in ['SUCCESS', 'SUCCESSFUL', '00']
        amount = int(float(order.get('amount') or trans.get('amount') or 0))
        source_app = trans.get('payment_channel') or 'DOKU QRIS'

    return str(mid) if mid else None, is_success, amount, source_app

@app.route('/')
def home():
    return jsonify({"status": "Server Soundbox Active", "message": "Endpoints: /webhook/payment and /events/<mid>"}), 200

@app.route('/webhook/payment', methods=['POST'])
def payment_webhook():
    data = request.json or {}
    mid, is_success, amount, source_app = extract_universal_payment(data)

    if mid and is_success and amount > 0:
        payload = {
            'amount': amount,
            'type': source_app
        }
        # Masukkan data pembayaran ke antrean spesifik MID
        merchant_queues[mid].put(payload)
        return jsonify({
            "status": "success",
            "message": f"Diterima QRIS {source_app} Rp{amount} untuk MID: {mid}"
        }), 200

    return jsonify({"status": "ignored", "message": "Transaksi diabaikan atau MID tidak valid"}), 200

@app.route('/events/<mid>')
def events_by_mid(mid):
    """
    Stream Server-Sent Events (SSE) khusus untuk MID tertentu.
    """
    def stream():
        q = merchant_queues[str(mid)]
        while True:
            data = q.get()
            yield f"data: {json.dumps(data)}\n\n"

    return Response(stream(), mimetype='text/event-stream')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
