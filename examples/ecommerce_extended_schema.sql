-- Extended E-commerce Schema for Testing
-- This schema includes more tables and relationships for comprehensive analysis

-- =====================
-- CORE TABLES
-- =====================

-- Users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    name VARCHAR(100),
    phone VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    last_active_at TIMESTAMP
);

-- User addresses (one-to-many with users)
CREATE TABLE addresses (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    address_type VARCHAR(20) DEFAULT 'shipping',
    street VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(50) DEFAULT 'USA',
    is_default BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- =====================
-- PRODUCT CATALOG
-- =====================

-- Product categories (self-referential for hierarchy)
CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    slug VARCHAR(100) NOT NULL UNIQUE,
    parent_id INTEGER REFERENCES categories(id),
    description TEXT,
    image_url VARCHAR(500),
    sort_order INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE
);

-- Brands
CREATE TABLE brands (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    slug VARCHAR(100) NOT NULL UNIQUE,
    logo_url VARCHAR(500),
    description TEXT
);

-- Products
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    short_description VARCHAR(500),
    price DECIMAL(10,2) NOT NULL,
    compare_at_price DECIMAL(10,2),
    cost DECIMAL(10,2),
    category_id INTEGER REFERENCES categories(id),
    brand_id INTEGER REFERENCES brands(id),
    inventory_count INTEGER DEFAULT 0,
    weight_grams INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    is_featured BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Product images (one-to-many with products)
CREATE TABLE product_images (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id),
    url VARCHAR(500) NOT NULL,
    alt_text VARCHAR(255),
    sort_order INTEGER DEFAULT 0,
    is_primary BOOLEAN DEFAULT FALSE
);

-- Product variants (sizes, colors, etc.)
CREATE TABLE product_variants (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id),
    sku VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(100),
    price_modifier DECIMAL(10,2) DEFAULT 0,
    inventory_count INTEGER DEFAULT 0,
    attributes JSONB
);

-- Product tags (many-to-many)
CREATE TABLE tags (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    slug VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE product_tags (
    product_id INTEGER NOT NULL REFERENCES products(id),
    tag_id INTEGER NOT NULL REFERENCES tags(id),
    PRIMARY KEY (product_id, tag_id)
);

-- =====================
-- ORDERS & CHECKOUT
-- =====================

-- Orders
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    order_number VARCHAR(50) NOT NULL UNIQUE,
    user_id INTEGER NOT NULL REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'pending',
    subtotal DECIMAL(10,2),
    tax DECIMAL(10,2),
    shipping_cost DECIMAL(10,2),
    discount DECIMAL(10,2) DEFAULT 0,
    total DECIMAL(10,2),
    shipping_address_id INTEGER REFERENCES addresses(id),
    billing_address_id INTEGER REFERENCES addresses(id),
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Order items (many-to-many between orders and products)
CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    product_id INTEGER NOT NULL REFERENCES products(id),
    variant_id INTEGER REFERENCES product_variants(id),
    quantity INTEGER NOT NULL DEFAULT 1,
    unit_price DECIMAL(10,2) NOT NULL,
    total_price DECIMAL(10,2) NOT NULL
);

-- Order status history
CREATE TABLE order_status_history (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    status VARCHAR(50) NOT NULL,
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- =====================
-- PAYMENTS
-- =====================

CREATE TABLE payments (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    payment_method VARCHAR(50),
    transaction_id VARCHAR(100),
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW()
);

-- =====================
-- SHOPPING CART
-- =====================

CREATE TABLE carts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    session_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE cart_items (
    id SERIAL PRIMARY KEY,
    cart_id INTEGER NOT NULL REFERENCES carts(id),
    product_id INTEGER NOT NULL REFERENCES products(id),
    variant_id INTEGER REFERENCES product_variants(id),
    quantity INTEGER NOT NULL DEFAULT 1,
    added_at TIMESTAMP DEFAULT NOW()
);

-- =====================
-- REVIEWS & RATINGS
-- =====================

-- Reviews (users review products)
CREATE TABLE reviews (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    product_id INTEGER NOT NULL REFERENCES products(id),
    order_id INTEGER REFERENCES orders(id),
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
    title VARCHAR(255),
    body TEXT,
    is_verified_purchase BOOLEAN DEFAULT FALSE,
    is_approved BOOLEAN DEFAULT FALSE,
    helpful_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Review images
CREATE TABLE review_images (
    id SERIAL PRIMARY KEY,
    review_id INTEGER NOT NULL REFERENCES reviews(id),
    url VARCHAR(500) NOT NULL
);

-- =====================
-- WISHLIST
-- =====================

CREATE TABLE wishlists (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    name VARCHAR(100) DEFAULT 'My Wishlist',
    is_public BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE wishlist_items (
    id SERIAL PRIMARY KEY,
    wishlist_id INTEGER NOT NULL REFERENCES wishlists(id),
    product_id INTEGER NOT NULL REFERENCES products(id),
    added_at TIMESTAMP DEFAULT NOW()
);

-- =====================
-- PROMOTIONS
-- =====================

CREATE TABLE coupons (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,
    description TEXT,
    discount_type VARCHAR(20), -- 'percentage' or 'fixed'
    discount_value DECIMAL(10,2),
    min_purchase DECIMAL(10,2),
    max_uses INTEGER,
    used_count INTEGER DEFAULT 0,
    starts_at TIMESTAMP,
    expires_at TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE order_coupons (
    order_id INTEGER NOT NULL REFERENCES orders(id),
    coupon_id INTEGER NOT NULL REFERENCES coupons(id),
    discount_amount DECIMAL(10,2),
    PRIMARY KEY (order_id, coupon_id)
);

-- =====================
-- INVENTORY & WAREHOUSES
-- =====================

CREATE TABLE warehouses (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE inventory (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id),
    variant_id INTEGER REFERENCES product_variants(id),
    warehouse_id INTEGER NOT NULL REFERENCES warehouses(id),
    quantity INTEGER NOT NULL DEFAULT 0,
    reserved_quantity INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (product_id, variant_id, warehouse_id)
);

-- =====================
-- EVENT LOGGING
-- =====================

CREATE TABLE event_logs (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    session_id VARCHAR(100),
    event_type VARCHAR(50) NOT NULL,
    event_data JSONB,
    page_url VARCHAR(500),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE session_events (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_data JSONB,
    timestamp TIMESTAMP DEFAULT NOW()
);

-- =====================
-- INDEXES
-- =====================

CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_addresses_user ON addresses(user_id);
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_products_brand ON products(brand_id);
CREATE INDEX idx_products_active ON products(is_active);
CREATE INDEX idx_product_images_product ON product_images(product_id);
CREATE INDEX idx_orders_user ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_created ON orders(created_at);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);
CREATE INDEX idx_reviews_product ON reviews(product_id);
CREATE INDEX idx_reviews_user ON reviews(user_id);
CREATE INDEX idx_cart_items_cart ON cart_items(cart_id);
CREATE INDEX idx_inventory_product ON inventory(product_id);
CREATE INDEX idx_event_logs_user ON event_logs(user_id);
CREATE INDEX idx_event_logs_created ON event_logs(created_at);
