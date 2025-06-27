# Final Project Big Data dan Data Lakehouse

**Anggota kelompok**:
| Nama      | NRP         |
|-----------|-------------|
| Wira Samudra Siregar  | 5027231041  |
| I Dewa Made Satya R   | 5027231051  |
| M. Syahmi Ash S  | 5027231085  |  
| Abid Ubaidillah Adam  | 5027231089  |


Dataset: https://www.kaggle.com/datasets/priyamchoksi/credit-card-transactions-dataset

# Monitoring Credit Card Transaction

## Arsitektur

![Image](https://github.com/user-attachments/assets/97fe739f-077e-446b-b5f5-f8a229193b3b)

Sistem ini dirancang untuk mendeteksi transaksi mencurigakan secara otomatis dan berjalan secara berkelanjutan. Alurnya dimulai dari data transaksi yang masuk melalui Kafka (streaming layer). Di sini, bagian consumer akan mengambil data secara bertahap (per 1000 baris) agar lebih mudah diproses.

Data yang sudah dibagi ke dalam batch disimpan ke MinIO, yaitu tempat penyimpanan file seperti cloud. Setelah itu, data dari MinIO akan digunakan oleh model machine learning yang dibuat dengan Python untuk mendeteksi apakah ada transaksi yang mencurigakan atau tidak.

Hasilnya akan ditampilkan melalui Streamlit, yaitu aplikasi web interaktif yang memudahkan admin bank melihat data dan hasil deteksi secara langsung. Semua komponen ini dijalankan dalam container menggunakan Docker agar mudah diatur dan dijalankan di berbagai lingkungan.

## Cara Menjalankannya

`docker compose up`

*script untuk menjalankan file yang lain sudah tertulis di dalam docker-compose.yml*

## Fitur-Fitur pada Dashboard
Dashboard ini dibuat dengan Streamlit dan digunakan oleh admin bank untuk memantau dan mendeteksi transaksi mencurigakan (fraud) secara langsung. Berikut fitur-fitur utama yang tersedia:

- Auto-refresh Dashboard
Pengguna dapat mengatur agar halaman dashboard memperbarui data secara otomatis setiap beberapa detik. Fitur ini penting agar admin bisa melihat data terbaru secara real-time tanpa perlu memuat ulang halaman secara manual.


- Pencarian Transaksi
Di sidebar, terdapat fitur pencarian berdasarkan nama depan atau belakang nasabah, nama merchant, dan kategori transaksi. Ini membantu admin menyaring data tertentu dengan cepat.

![Screenshot_2025-06-27_174227 1](https://github.com/user-attachments/assets/07f18e97-1ba2-4b64-8652-402e1b45b6f5)


- Statistik Dataset
Dashboard menampilkan ringkasan statistik seperti jumlah total transaksi dan jumlah transaksi yang terdeteksi sebagai fraud. Ini memberikan gambaran umum tentang skala dan tingkat kecurangan yang terpantau.

- Visualisasi Data
Dashboard menyediakan grafik batang (bar chart) untuk menunjukkan distribusi kategori transaksi dan daftar merchant paling sering muncul. Hal ini membantu dalam memahami pola transaksi.

![image](https://github.com/user-attachments/assets/120f0192-6168-4e0b-baf2-52699c2ad8b3)


- Prediksi Transaksi Baru
Admin bisa memasukkan data transaksi baru secara manual seperti jumlah uang (amt), kategori, merchant, dan gender pengguna. Setelah itu, sistem akan memprediksi apakah transaksi tersebut normal atau berpotensi fraud menggunakan model machine learning yang telah dilatih sebelumnya.

Transaksi Normal:

![Screenshot_2025-06-27_174511 1](https://github.com/user-attachments/assets/82c14853-8161-4e75-86da-b471404d4587)

Transaksi Fraud:

![Screenshot_2025-06-27_171903 1](https://github.com/user-attachments/assets/a22f29c5-0f77-473d-bc1c-464673e615f4)


- Integrasi dengan MinIO
Semua data transaksi (batch) dan model machine learning diambil langsung dari MinIO, sebuah penyimpanan objek mirip S3. Dengan ini, dashboard selalu menampilkan data terbaru yang disimpan dari hasil proses Kafka consumer.

![Screenshot_2025-06-27_174905 1](https://github.com/user-attachments/assets/a2c4e076-4885-42d3-9475-887e83b09187)

