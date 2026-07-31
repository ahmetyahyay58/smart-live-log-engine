import json
import os
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# 1. YARDIMCI VERİ ÜRETİCİ FONKSİYON
# ==========================================
def tekil_sunger_logu_uret(bozuk_veri_olsun_mu=False):
    """
    Form Sünger döküm hattı için simüle edilmiş tekil üretim logu üretir.
    bozuk_veri_olsun_mu=True ise %10 ihtimalle karantina uyarısı verecek veri basar.
    """
    blok_id = f"FS-2026-{random.randint(1000, 9999)}"
    sunger_tipleri = [
        'Visco (Akilli Sunger)', 
        'HR (Yüksek Esneklikli)', 
        'Standart Poliuretan', 
        'Yanmaz Sunger'
    ]
    
    # Varsayılan Temiz Değerler
    sunger_tipi = random.choice(sunger_tipleri)
    dansite = round(random.uniform(18.0, 60.0), 2)
    sertlik = round(random.uniform(2.5, 6.0), 2)
    sicaklik = round(random.uniform(20.0, 45.0), 2)
    blok_uzunluk = random.randint(180, 220)
    blok_agirlik = round(random.uniform(120.0, 250.0), 2)
    kalite_durumu = random.choice(['Onaylandi', 'Onaylandi', 'Onaylandi', 'Yeniden Islem', 'Fire/Iskarta'])
    
    hata_turu = 'Yok'
    if kalite_durumu != 'Onaylandi':
        hata_turu = random.choice([
            'Hava Boslugu (Gozenek Hatasi)', 
            'Dansite Duzensizligi', 
            'Kabuk Yapismasi'
        ])

    # Karantina Senaryoları (Bozuk Veri Üretimi)
    if bozuk_veri_olsun_mu:
        hata_senaryosu = random.choice(['sicaklik_sensor_arizasi', 'dansite_format_hatasi', 'eksik_veri_null'])
        
        if hata_senaryosu == 'sicaklik_sensor_arizasi':
            sicaklik = round(random.uniform(1050.0, 1500.0), 2)  # Aşırı yüksek sıcaklık
        elif hata_senaryosu == 'dansite_format_hatasi':
            dansite = "HATALI_STR_DANSITE"  # Sayı yerine metin
        elif hata_senaryosu == 'eksik_veri_null':
            sunger_tipi = None  # Boş/NULL değer

    log = {
        "blok_id": blok_id,
        "sunger_tipi": sunger_tipi,
        "dansite_kg_m3": dansite,
        "sertlik_kpa": sertlik,
        "kimyasal_sicaklik_c": sicaklik,
        "blok_uzunluk_cm": blok_uzunluk,
        "blok_agirlik_kg": blok_agirlik,
        "kalite_durumu": kalite_durumu,
        "hata_turu": hata_turu,
        "zaman_damgasi": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    return log

# ==========================================
# 2. REDPANDA PRODUCER VE CANLI AKIŞ MANTIĞI
# ==========================================
REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "sunger_uretim_stream")

def producer_baslat():
    print("--------------------------------------------------")
    print("🚀 REDPANDA CANLI DÖKÜM HATTI PRODUCER BAŞLATILDI")
    print("--------------------------------------------------\n")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=[REDPANDA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"✅ Redpanda Bağlantısı Başarılı: {REDPANDA_BROKER}\n")
    except Exception as e:
        print(f"❌ Redpanda Bağlantı Hatası: {e}")
        return

    print("📡 Canlı döküm hattı yayını başladı (2 saniyede bir 1 veri)... (Ctrl+C ile durdurulabilir)\n")
    
    sayac = 1
    try:
        while True:
            bozuk_mu = random.random() < 0.10
            ham_log = tekil_sunger_logu_uret(bozuk_veri_olsun_mu=bozuk_mu)
            
            producer.send(TOPIC_NAME, value=ham_log)
            print(f"📤 #{sayac} [REDPANDA'YA ATILDI]: {ham_log['blok_id']} | Tip: {ham_log.get('sunger_tipi')} | Temp: {ham_log.get('kimyasal_sicaklik_c')}°C")
            
            sayac += 1
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\n🛑 Döküm hattı canlı yayını durduruldu.")

if __name__ == "__main__":
    producer_baslat()