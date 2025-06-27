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
![Frame 1 (1)](https://github.com/user-attachments/assets/7ef3a0b7-666b-43be-a77c-9f01c6bddc85)

Sistem ini dirancang untuk mendeteksi transaksi mencurigakan secara otomatis dan berjalan secara berkelanjutan. Alurnya dimulai dari data transaksi yang masuk melalui Kafka (streaming layer). Di sini, bagian consumer akan mengambil data secara bertahap (per 1000 baris) agar lebih mudah diproses.

Data yang sudah dibagi ke dalam batch disimpan ke MinIO, yaitu tempat penyimpanan file seperti cloud. Setelah itu, data dari MinIO akan digunakan oleh model machine learning yang dibuat dengan Python untuk mendeteksi apakah ada transaksi yang mencurigakan atau tidak.

Hasilnya akan ditampilkan melalui Streamlit, yaitu aplikasi web interaktif yang memudahkan admin bank melihat data dan hasil deteksi secara langsung. Semua komponen ini dijalankan dalam container menggunakan Docker agar mudah diatur dan dijalankan di berbagai lingkungan.


##
