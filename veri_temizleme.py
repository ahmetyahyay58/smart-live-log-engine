import os

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import json
from kafka import KafkaConsumer
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from collections import deque
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# 1. BAĞLANTI BİLGİLERİ (PostgreSQL & Redpanda)
# ==========================================
VERITABANI_URL = os.getenv("VERITABANI_URL")
motor = create_engine(VERITABANI_URL)

REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "sunger_uretim_stream")

# ==========================================
# 2. E-POSTA BİLDİRİM AYARLARI
# ==========================================
GONDEREN_EMAIL = os.getenv("GONDEREN_EMAIL")
UYGULAMA_SIFRESI = os.getenv("UYGULAMA_SIFRESI")
ALICI_EMAIL = os.getenv("ALICI_EMAIL")
# Son 1 dakikadaki karantina zamanlarını tutacak hafıza
karantina_zamanlari = deque()

def eposta_yogunluk_alarmi_gonder(hata_sayisi, son_hatalar):
    """
    Son 1 dakikada karantina sayısı 2'yi geçtiğinde özet uyarı maili fırlatır.
    """
    konu = f"🚨 KRİTİK HAT YOĞUNLUĞU: Son 1 Dakikada {hata_sayisi} Karantina Hatası!"
    
    # Mail içinde son hataların kısa dökümünü hazırlıyoruz
    hata_listesi_html = ""
    for h in son_hatalar:
        hata_listesi_html += f"<li><b>Blok ID:</b> {h['blok_id']} | <b>Neden:</b> <span style='color: red;'>{h['karantina_nedeni']}</span></li>"

    html_icerik = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
        <div style="background-color: #f8d7da; padding: 15px; border-radius: 5px; border: 1px solid #f5c6cb;">
          <h2 style="color: #721c24; margin-top: 0;">⚠️ Döküm Hattında Anomali Yoğunluğu Tespit Edildi!</h2>
          <p>Son 1 dakika içinde <b>{hata_sayisi} adet</b> veri kalitesi hatası/sensör arızası yakalandı. Üretim hattında kronik bir problem olabilir.</p>
        </div>
        
        <h3>📋 Son Yakalanan Hata Örnekleri:</h3>
        <ul>
          {hata_listesi_html}
        </ul>
        
        <p><i>Lütfen Metabase panosunu ve saha sensörlerini kontrol edin.</i></p>
        <hr>
        <p style="font-size: 0.85em; color: #777;">
          <i>Bu e-posta Form Sünger Real-Time Windowing Pipeline tarafından otomatik üretilmiştir.</i>
        </p>
      </body>
    </html>
    """

    msg = MIMEMultipart()
    msg['From'] = GONDEREN_EMAIL
    msg['To'] = ALICI_EMAIL
    msg['Subject'] = konu
    msg.attach(MIMEText(html_icerik, 'html'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(GONDEREN_EMAIL, UYGULAMA_SIFRESI)
        server.send_message(msg)
        server.quit()
        print(f"\n📧 [YOĞUNLUK ALARMI MAİLİ ATILDI] -> Son 1 dakikada {hata_sayisi} hata tespit edildi!\n")
    except Exception as e:
        print(f"\n❌ E-posta gönderilemedi: {e}\n")

# ==========================================
# 3. SÜZGEÇ / KARANTİNA FONKSİYONU
# ==========================================
def veriyi_suz_ve_ayristir(ham_log):
    """
    Ham logu inceler. Hatalıysa spesifik nedenini ekleyip KARANTİNA durumuna ayırır.
    """
    hata_nedeni = None
    
    # 1. Kontrol: Sensör Sıcaklık Hatası
    if ham_log.get('kimyasal_sicaklik_c', 0) > 1000:
        hata_nedeni = f"Kritik Sensör Arızası: Aşırı Sıcaklık ({ham_log.get('kimyasal_sicaklik_c')}°C)"
        
    # 2. Kontrol: Format Hatası (Sayı yerine metin gelmesi)
    elif isinstance(ham_log.get('dansite_kg_m3'), str):
        hata_nedeni = f"Format Hatası: Dansite metin olarak gönderildi ('{ham_log.get('dansite_kg_m3')}')"
        
    # 3. Kontrol: Eksik Veri Hatası (Sünger tipinin boş kalması)
    elif ham_log.get('sunger_tipi') is None:
        hata_nedeni = "Eksik Veri: Sünger tipi boş (NULL)"
        
    # Yönlendirme Kararı
    if hata_nedeni:
        karantina_kaydi = ham_log.copy()
        karantina_kaydi['karantina_nedeni'] = hata_nedeni
        karantina_kaydi['karantina_zamani'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return "KARANTINA", karantina_kaydi
    else:
        return "TEMIZ", ham_log

# ==========================================
# 4. REDPANDA CONSUMER (CANLI VERİ AKIŞI DİNLEYİCİ)
# ==========================================
def temizleme_motorunu_baslat():
    print("--------------------------------------------------")
    print("🛡️ REDPANDA DESTEKLİ VERİ TEMİZLEME MOTORU BAŞLATILDI")
    print("--------------------------------------------------\n")
    
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[REDPANDA_BROKER],
            auto_offset_reset='latest',  # Canlı gelen verileri dinler
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print(f"📥 Redpanda Dinleniyor... Broker: {REDPANDA_BROKER} | Topic: '{TOPIC_NAME}'\n")
    except Exception as e:
        print(f"❌ Redpanda Consumer Bağlantı Hatası: {e}")
        return

    son_hatalar_listesi = []

    try:
        for message in consumer:
            ham_log = message.value
            durum, veri = veriyi_suz_ve_ayristir(ham_log)
            
            df_tekil = pd.DataFrame([veri])
            
            if durum == "TEMIZ":
                df_tekil.to_sql('dokum_uretim_loglari', con=motor, if_exists='append', index=False)
                print(f"🟢 [TEMİZ VERİ VT'YE YAZILDI]: {veri['blok_id']} | {veri['sunger_tipi']} | Dansite: {veri['dansite_kg_m3']}")
            else:
                df_tekil.to_sql('hatali_loglar_karantina', con=motor, if_exists='append', index=False)
                print(f"⚠️ [KARANTİNAYA ALINDI]: {veri['blok_id']} | Sebep: {veri['karantina_nedeni']}")
                
                # --- ZAMAN PENCERESİ (WINDOWING) KONTROLÜ ---
                simdi = datetime.now()
                karantina_zamanlari.append(simdi)
                son_hatalar_listesi.append(veri)

                # 1 dakikadan (60 saniye) eski olan zaman kayıtlarını hafızadan temizle
                while karantina_zamanlari and (simdi - karantina_zamanlari[0]) > timedelta(seconds=60):
                    karantina_zamanlari.popleft()
                    if son_hatalar_listesi:
                        son_hatalar_listesi.pop(0)

                # Eğer son 1 dakikadaki hata sayısı 2'yi geçerse (3 veya daha fazla) alarm ver!
                if len(karantina_zamanlari) > 2:
                    eposta_yogunluk_alarmi_gonder(
                        hata_sayisi=len(karantina_zamanlari), 
                        son_hatalar=son_hatalar_listesi
                    )
                    # Mail attıktan sonra aynı hatalar için tekrar tekrar mail atmaması için hafızayı sıfırlıyoruz
                    karantina_zamanlari.clear()
                    son_hatalar_listesi.clear()

    except KeyboardInterrupt:
        print("\n🛑 Veri temizleme motoru kullanıcı tarafından durduruldu.")

if __name__ == "__main__":
    temizleme_motorunu_baslat()
