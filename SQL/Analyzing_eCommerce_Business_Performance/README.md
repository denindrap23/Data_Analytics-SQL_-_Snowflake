# Analyzing eCommerce Business Performance with SQL

## Deskripsi Project
Pada project ini, saya berperan sebagai anggota tim **Data Analytics** di sebuah perusahaan eCommerce besar di Amerika Selatan.  
Perusahaan ini merupakan salah satu marketplace terbesar di wilayah tersebut, menghubungkan pelaku usaha mikro dengan jutaan pelanggan.  
Tugas saya adalah menganalisis **tiga aspek kunci performa bisnis** untuk membantu manajemen mengambil keputusan strategis.

Ketiga aspek yang saya analisis adalah:
1. **Pertumbuhan Pelanggan**
2. **Kualitas Produk**
3. **Tipe Pembayaran**

Analisis ini dilakukan menggunakan **SQL** dengan memanfaatkan 9 dataset yang sudah disediakan dalam format CSV.

---

## Dataset
Berikut adalah penjelasan detail setiap dataset:

| Dataset        | Deskripsi |
|----------------|-----------|
| `customers_dataset.csv`  | Informasi pelanggan, termasuk ID pelanggan dan lokasi. |
| `geolocation_dataset` | Data koordinat geografis (latitude, longitude) yang dikaitkan dengan kode pos. |
| `order_items_dataset.csv` | Detail barang pada setiap pesanan, termasuk harga, biaya pengiriman, dan ID produk. |
| `order_payments_dataset` | Data transaksi pembayaran, termasuk metode pembayaran, nilai pembayaran, dan jumlah cicilan. |
| `order_reviews_dataset` | Ulasan pelanggan, termasuk rating dan komentar. |
| `orders_dataset.csv` | Data pesanan pelanggan, termasuk tanggal pembelian, status, dan waktu pengiriman. |
| `product_dataset.csv` | Informasi produk, termasuk kategori, ukuran, dan detail lainnya. |
| `sellers_dataset` | Informasi penjual, termasuk ID penjual dan lokasi. |

---

## Tujuan Analisis

### 1. **Annual Customer Activity Growth Analysis**
**Tujuan:**
- Mengukur pertumbuhan jumlah pelanggan aktif setiap tahun.
- Mengidentifikasi tren perilaku pelanggan dari waktu ke waktu.
- Memberikan insight apakah strategi marketing dan retensi pelanggan berjalan efektif.

---

### 2️. **Annual Product Category Quality Analysis**
**Tujuan:**
- Mengukur kualitas produk berdasarkan rating ulasan pelanggan.
- Mengidentifikasi kategori produk dengan kualitas terbaik dan terburuk.
- Mengetahui apakah ada penurunan kualitas pada kategori tertentu dari tahun ke tahun.

---

### 3️. **Analysis of Annual Payment Type Usage**
**Tujuan:**
- Menganalisis tren penggunaan metode pembayaran setiap tahun.
- Mengidentifikasi metode pembayaran yang paling populer dan yang mulai jarang digunakan.
- Mengetahui apakah preferensi pembayaran berubah dari waktu ke waktu.

---

## Proses Pengerjaan
### 1. **Data Preparation**
   - Membuat workspace database.

    CREATE TABLE customers (
	    customer_id VARCHAR(250),
	    customer_unique_id VARCHAR(250),
	    customer_zip_code_prefix INT,
	    customer_city VARCHAR(250),
	    customer_state VARCHAR(250)
    );

    CREATE TABLE geolocation (
	    geo_zip_code_prefix VARCHAR(250),
	    geo_lat VARCHAR(250),
	    geo_lng VARCHAR(250),
	    geo_city VARCHAR(250),
	    geo_state VARCHAR(250)
    );

    CREATE TABLE order_item (
	    order_id VARCHAR(250),
	    order_item_id INT,
	    product_id VARCHAR(250),
	    seller_id VARCHAR(250),
	    shipping_limit_date TIMESTAMP,
	    price FLOAT,
	    freight_value FLOAT
    );

    CREATE TABLE payments (
	    order_id VARCHAR(250),
	    payment_sequential INT,
	    payment_type VARCHAR(250),
	    payment_installment INT,
	    payment_value FLOAT
    );

    CREATE TABLE reviews (
	    review_id VARCHAR(250),
	    order_id VARCHAR(250),
	    review_score INT, 
	    review_comment_title VARCHAR(250),
	    review_comment_message TEXT,
	    review_creation_date TIMESTAMP,
	    review_answer TIMESTAMP
    );

    CREATE TABLE orders (
	    order_id VARCHAR(250),
	    customers_id VARCHAR(250),
	    order_status VARCHAR(250),
	    order_purchase_timestamp TIMESTAMP,
	    order_approved_at TIMESTAMP,
	    order_delivered_carrier_date TIMESTAMP,
	    order_delivered_customer_date TIMESTAMP,
	    order_estimated_delivered_date TIMESTAMP
    );

    CREATE TABLE products (
	    product_id VARCHAR(250),
	    product_category_name VARCHAR(250),
	    product_name_length INT,
	    product_description_length INT,
	    product_photos_qty INT,
	    product_weight_g INT,
	    product_length_cm INT,
	    product_height_cm INT,
	    product_width_cm INT
    );

    CREATE TABLE sellers (
	    seller_id VARCHAR(250),
	    seller_zip_code INT,
	    seller_city VARCHAR(250),
    	seller_state VARCHAR(250)
    );
	
	
   - Mengimpor semua dataset CSV ke dalam database.

	COPY customers (
         customer_id,
         customer_unique_id,
     	 customer_zip_code_prefix,
     	 customer_city,
     	 customer_state
   	)
   	FROM 'C:\Users\denindra\Documents\Project\SQL\Dataset\customers_dataset.csv'
   	DELIMITER ','
   	CSV HEADER;

   	COPY geolocation (
	     geo_zip_code_prefix,
	     geo_lat,
	     geo_lng,
	     geo_city,
	     geo_state
   	)
   	FROM 'C:\Users\denindra\Documents\Project\SQL\Dataset\geolocation_dataset.csv'
   	DELIMITER ','
   	CSV HEADER;

   	COPY order_item (
	     order_id,
	     order_item_id,
	     product_id,
	     seller_id,
	     shipping_limit_date,
	     price,
	     freight_value
   	)
   	FROM 'C:\Users\denindra\Documents\Project\SQL\Dataset\order_items_dataset.csv'
   	DELIMITER ','
   	CSV HEADER;

   	COPY payments (
	     order_id,
	     payment_sequential,
	     payment_type,
	     payment_installment,
	     payment_value
   	)
   	FROM 'C:\Users\denindra\Documents\Project\SQL\Datasetorder_payments_dataset.csv'
   	DELIMITER ','
   	CSV HEADER;

   	COPY reviews (
	     review_id,
	     order_id,
	     review_score, 
	     review_comment_title,
	     review_comment_message,
	     review_creation_date,
	     review_answer
   	)
   	FROM 'C:\Users\denindra\Documents\Project\SQL\Dataset\order_reviews_dataset.csv'
   	DELIMITER ','
   	CSV HEADER;

   	COPY orders (
	     order_id,
	     customers_id,
	     order_status,
	     order_purchase_timestamp,
	     order_approved_at,
	     order_delivered_carrier_date,
	     order_delivered_customer_date,
	     order_estimated_delivered_date
   	)
   	FROM 'C:\Users\denindra\Documents\Project\SQL\Dataset\orders_dataset.csv'
   	DELIMITER ','
   	CSV HEADER;

   	COPY products (
	     product_id,
	     product_category_name,
	     product_name_length,
	     product_description_length,
	     product_photos_qty,
	     product_weight_g,
	     product_length_cm,
	     product_height_cm,
	     product_width_cm
   	)
   	FROM 'C:\Users\denindra\Documents\Project\SQL\Dataset\product_dataset.csv'
   	DELIMITER ','
  	CSV HEADER;

  	COPY sellers (
	     seller_id,
	     seller_zip_code,
	     seller_city,
       	 seller_state
    )
   	FROM 'C:\Users\denindra\Documents\Project\SQL\Dataset\sellers_dataset.csv'
    DELIMITER ','
    CSV HEADER;
    
	
   - Membuat **Entity Relationship Diagram (ERD)** untuk memahami hubungan antar tabel.

	
    ## Primary Key
    ALTER TABLE products ADD CONSTRAINT pk_products PRIMARY KEY (product_id);
    ALTER TABLE order_items ADD FOREIGN KEY (product_id) REFERENCES products;
    ALTER TABLE customers ADD CONSTRAINT pk_cust PRIMARY KEY (customer_id);
    ALTER TABLE geolocation ADD CONSTRAINT pk_geo PRIMARY KEY (geo_zip_code_prefix);
    ALTER TABLE orders ADD CONSTRAINT pk_orders PRIMARY KEY (order_id);
    ALTER TABLE sellers ADD CONSTRAINT pk_seller PRIMARY KEY (seller_id);

    ## Foreign Key
    ALTER TABLE customers ADD FOREIGN KEY (customer_zip_code_prefix) REFERENCES geolocation;
    ALTER TABLE orders ADD FOREIGN KEY (customer_id) REFERENCES customers;
    ALTER TABLE order_items ADD FOREIGN KEY (order_id) REFERENCES orders;
    ALTER TABLE order_items ADD FOREIGN KEY (seller_id) REFERENCES sellers;
    ALTER TABLE sellers ADD FOREIGN KEY (seller_zip_code_prefix) REFERENCES geolocation;
    ALTER TABLE payments ADD FOREIGN KEY (order_id) REFERENCES orders;
    ALTER TABLE order_items ADD FOREIGN KEY (product_id) REFERENCES products;
    ALTER TABLE reviews ADD FOREIGN KEY (order_id) REFERENCES orders;
    
	
 	![ERD](image/ERD.png)

### 2. SQL Query Development
#### 2.1. Annual Customer Activity Growth Analysis
  - Query

     
     WITH 
     ## Rata-rata jumlah customer aktif bulanan (monthly active user)
     calc_mau AS (
     SELECT
       year,
       round(AVG(mau), 2) AS average_mau
     FROM (
       SELECT
         date_part('year', o.order_purchase_timestamp) AS year,
         date_part('month', o.order_purchase_timestamp) AS month,
         COUNT(distinct c.customer_unique_id) AS mau
       FROM orders o
       JOIN customers c ON o.customer_id = c.customer_id
       GROUP BY 1,2
       ) subq
     GROUP BY 1
     ),

     ## Jumlah customer baru (pertama kali bertransaksi)
     calc_newcust AS (
     SELECT
       date_part('year', first_purchase_time) AS year,
       COUNT(1) AS new_customers
     FROM (
       SELECT
         c.customer_unique_id,
         min(o.order_purchase_timestamp) AS first_purchase_time
       FROM orders o
       JOIN customers c ON c.customer_id = o.customer_id
       GROUP BY 1
     ) subq
     GROUP BY 1
     ),

     ## Jumlah customer yang melakukan pembelian lebih dari satu kali (repeat order)
     calc_repeat AS (
     SELECT
       year,
       COUNT(distinct customer_unique_id) AS repeating_customers
     FROM (
       SELECT
         date_part('year', o.order_purchase_timestamp) AS year,
         c.customer_unique_id,
         COUNT(1) AS purchase_frequency
       FROM orders o
       JOIN customers c ON c.customer_id = o.customer_id
       GROUP BY 1, 2
       HAVING COUNT(1) > 1
     ) subq
     GROUP BY 1
     ),

     ## Rata-rata jumlah order yang dilakukan customer
     calc_avg_freq AS (
     SELECT
       year,
       ROUND(AVG(frequency_purchase),3) AS avg_orders_per_customers
     FROM (
       SELECT
         date_part('year', o.order_purchase_timestamp) AS year,
         c.customer_unique_id,
         COUNT(1) AS frequency_purchase
       FROM orders o
       JOIN customers c ON c.customer_id = o.customer_id
       GROUP BY 1, 2
     ) a
     GROUP BY 1
     )

     ## Menggabungkan ketiga metrik
     SELECT
       mau.year,
       mau.average_mau,
       newc.new_customers,
       rep.repeating_customers,
       freq.avg_orders_per_customers
     FROM calc_mau mau
     JOIN calc_newcust newc ON mau.year = newc.year
     JOIN calc_repeat rep ON rep.year = mau.year
     JOIN calc_avg_freq freq ON freq.year = mau.year
     

  - Result
      
  - Analysis
    
       
