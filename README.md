# postgres-bigquery-sync-etl
This project connects to PostgreSQL and BigQuery to perform Extract, Transform, Load (ETL), as well as data synchronization and cleansing. The process includes:
- Extracting data from PostgreSQL and BigQuery based on certain transaction date conditions.
- Loading data from PostgreSQL into BigQuery into a staging table.
- Using MERGE to update or insert data into the target table in BigQuery.
- Comparing data between PostgreSQL and BigQuery to identify IDs present in BigQuery but not in PostgreSQL.
- Deleting irrelevant data in BigQuery.
- Optimizing the process using batch processing and automatic duplicate checks.

------------------------------------------------------------------------------------

Project ini menghubungkan ke database PostgreSQL dan BigQuery untuk melakukan Extract, Transform, Load (ETL), serta sinkronisasi dan pembersihan data. Proses yang dilakukan meliputi:
- Mengekstrak data dari PostgreSQL dan BigQuery berdasarkan kondisi tanggal transaksi tertentu.
- Memuat data dari PostgreSQL ke BigQuery dalam tabel staging.
- Menggunakan MERGE untuk memperbarui atau menambahkan data ke dalam tabel target di BigQuery.
- Membandingkan data antara PostgreSQL dan BigQuery untuk mengidentifikasi ID yang ada di BigQuery tetapi tidak ada di PostgreSQL.
- Menghapus data yang tidak relevan di BigQuery.
- Mengoptimalkan proses dengan menggunakan batch dan pengecekan duplikasi otomatis.
