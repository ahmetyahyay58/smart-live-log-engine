import os
import psycopg2
import pandas as pd
from groq import Groq
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
# ==========================================
# 1. BİLGİLER VE BAĞLANTILAR
# ==========================================
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "form_sunger_db")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")

# ==========================================
# 2. METABASE İÇİN RAPORU DB'YE KAYDETME (DÜZELTİLDİ)
# ==========================================
def ai_raporunu_db_kaydet(rapor_metni: str):
    """
    AI'ın ürettiği raporu Metabase'in okuyabilmesi için PostgreSQL'e yazar.
    Zaman damgasını anlık saniyesiyle kaydeder.
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cursor = conn.cursor()
        
        # Tablo yoksa oluşturuyoruz
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ai_analiz_sonuclari (
                id SERIAL PRIMARY KEY,
                zaman_damgasi TIMESTAMP,
                tam_rapor TEXT
            );
        """)
        
        # Anlık zamanı saniyesiyle alıp veritabanına kaydediyoruz
        suan = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        cursor.execute("""
            INSERT INTO ai_analiz_sonuclari (zaman_damgasi, tam_rapor)
            VALUES (%s, %s)
        """, (suan, rapor_metni))
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"\n💾 [METABASE ENTEGRASYONU] AI Raporu kaydedildi! (Zaman: {suan})")
    except Exception as e:
        print(f"\n❌ AI Raporu DB'ye kaydedilirken hata oluştu: {e}")

# ==========================================
# 3. AI TOOL (SQL ÇALIŞTIRICI)
# ==========================================
def sql_sorgusu_calistir(sql_query: str) -> str:
    """
    PostgreSQL veritabanında AI tarafından üretilen SQL sorgusunu çalıştırır.
    """
    print(f"\n🕵️‍♂️ [AI DEDEKTİF SORGUSU] AI Şu SQL'i Çalıştırıyor:\n👉 {sql_query}\n")
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        if not sql_query.strip().lower().startswith("select"):
            return "HATA: Güvenlik nedeniyle sadece SELECT sorguları çalıştırılabilir."
            
        df = pd.read_sql(sql_query, conn)
        conn.close()
        
        if df.empty:
            return "Sorgu çalıştı ancak sonuç boş döndü."
        return df.to_string(index=False)
    except Exception as e:
        return f"SQL Hatanız: {str(e)}"

# ==========================================
# 4. VERİTABANI ŞEMASI
# ==========================================
VERITABANI_SEMASI = """
Veritabanında 2 ana üretim/karantina tablosu bulunmaktadır:

1. TABLO: `dokum_uretim_loglari` (Temiz ve işlenmiş üretim verileri)
   - `blok_id` (TEXT, Örn: FS-2026-1029)
   - `sunger_tipi` (TEXT, Örn: 'Visco (Akilli Sunger)', 'HR (Yüksek Esneklikli)', 'Standart Poliuretan', 'Yanmaz Sunger')
   - `dansite_kg_m3` (NUMERIC, Yoğunluk)
   - `sertlik_kpa` (NUMERIC, Sertlik değeri)
   - `kimyasal_sicaklik_c` (NUMERIC, °C cinsinden reaksiyon sıcaklığı)
   - `blok_uzunluk_cm` (INTEGER)
   - `blok_agirlik_kg` (NUMERIC)
   - `kalite_durumu` (TEXT, 'Onaylandi', 'Yeniden Islem', 'Fire/Iskarta')
   - `hata_turu` (TEXT, 'Hava Boslugu (Gozenek Hatasi)', 'Dansite Duzensizligi', 'Kabuk Yapismasi', 'Yok')
   - `zaman_damgasi` (TIMESTAMP)

2. TABLO: `hatali_loglar_karantina` (Veri kalitesi bozuk karantina logları)
   - `blok_id` (TEXT)
   - `sunger_tipi` (TEXT)
   - `dansite_kg_m3` (TEXT veya NUMERIC)
   - `kimyasal_sicaklik_c` (NUMERIC)
   - `karantina_nedeni` (TEXT, Örn: 'Kritik Sensör Arızası: Aşırı Sıcaklık...', 'Format Hatası...', 'Eksik Veri...')
   - `karantina_zamani` (TIMESTAMP)
"""

tools = [
    {
        "type": "function",
        "function": {
            "name": "sql_sorgusu_calistir",
            "description": "Veritabanından canlı veri analiz etmek için PostgreSQL SELECT sorgusu çalıştırır.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql_query": {
                        "type": "string",
                        "description": "Çalıştırılacak SQL SELECT sorgusu."
                    }
                },
                "required": ["sql_query"]
            }
        }
    }
]

# ==========================================
# 5. OTONOM DEDEKTİF DÖNGÜSÜ
# ==========================================
def ai_otonom_analiz():
    print("🤖 Groq destekli AI Veri Mühendisi (Dedektif Modu) Başlatılıyor...")
    print("🔍 AI Redpanda ve PostgreSQL verilerini çapraz sorguluyor...\n")

    client = Groq(api_key=os.getenv("GROQ_API_KEY"))
    
    system_prompt = f"""
    Sen Form Sünger Fabrikasının Baş Veri Mühendisi ve Kalite Dedektifisin.
    Sana veritabanı şeması ve `sql_sorgusu_calistir` adında bir araç verilmiştir.

    ŞEMA BİLGİSİ:
    {VERITABANI_SEMASI}

    ANALİZ STRATEJİN:
    1. Hem `dokum_uretim_loglari` hem de `hatali_loglar_karantina` tablolarını sorgula.
    2. Karantinaya düşen verilerin ana sebeplerini ve üretim kalitesindeki ('Fire/Iskarta') ana eğilimleri tespit et.
    3. Parametreler (`kimyasal_sicaklik_c`, `dansite_kg_m3`, `sertlik_kpa`) arasındaki sapmaları yakala.

    ⚠️ KURALLAR:
    - SQL sorgularında kolon isimlerini şemaya birebir uygun yaz (`kimyasal_sicaklik_c`, `dansite_kg_m3` vb.).
    - Her sorguna `LIMIT 10` veya `GROUP BY` ekle.
    
    Raporunu Fabrika Müdürüne şu formatta sun:
    🎯 **1. TEŞHİS VE KÖK NEDEN** (Karantina ve Fire durumlarının ana sebebi)
    🔬 **2. SENSÖR VE PARAMETRE ANALİZİ** (Sıcaklık, dansite ve sertlik sapmaları)
    🛠️ **3. ACİL SAHA AKSİYONLARI** (Fabrika ekibinin alması gereken önlemler)
    """

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": "Canlı akıştan veritabanına gelen verileri, karantina sebeplerini ve kalite durumlarını detaylıca inceleyip kök neden raporu oluştur."}
    ]

    max_tur = 5
    for _ in range(max_tur):
        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=messages,
            tools=tools,
            tool_choice="auto",
            temperature=0.1
        )

        response_message = response.choices[0].message
        messages.append(response_message)

        if response_message.tool_calls:
            for tool_call in response_message.tool_calls:
                function_args = json.loads(tool_call.function.arguments)
                query = function_args.get("sql_query")
                
                db_result = sql_sorgusu_calistir(query)

                messages.append({
                    "tool_call_id": tool_call.id,
                    "role": "tool",
                    "name": "sql_sorgusu_calistir",
                    "content": db_result
                })
        else:
            rapor = response_message.content
            
            print("\n==================================================")
            print("🤖 AI KÖK NEDEN VE TEŞHİS RAPORU")
            print("==================================================\n")
            print(rapor)
            print("\n==================================================")
            
            # --- METABASE ENTEGRASYONU ---
            ai_raporunu_db_kaydet(rapor)
            break

if __name__ == "__main__":
    ai_otonom_analiz()