import sqlite3
import os
from datetime import datetime, timezone

# Assuming DB_NAME is defined here or imported from storage
DB_NAME = 'queuectl.db'

def get_db_connection():
    """Returns a connection object to the database."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_config_db():
    """Create the configuration table if it doesn't exist."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    ''')
    conn.commit()
    conn.close()

init_config_db()

def set_config(key: str, value: str):
    """Set a configuration key-value pair persistently."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT OR REPLACE INTO config (key, value)
        VALUES (?, ?)
    ''', (key, value))
    
    conn.commit()
    conn.close()

def get_config(key: str, default: str | None = None) -> str | None:
    """Retrieve a configuration value by key."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT value FROM config WHERE key = ?", (key,))
    row = cursor.fetchone()
    
    conn.close()
    
    if row:
        return row['value']
    return default

def get_all_config() -> dict:
    """Retrieve all configuration settings."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT key, value FROM config")
    config_dict = {row['key']: row['value'] for row in cursor.fetchall()}
    
    conn.close()
    return config_dict