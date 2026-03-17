#!/usr/bin/env python3
"""
Simple symmetric encryption/decryption tool using AES-256-GCM.

Usage:
    python crypto.py encrypt "plaintext" "password"
    python crypto.py decrypt "base64cipher" "password"
"""

import sys
import base64
import os
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


def derive_key(password: str, salt: bytes) -> bytes:
    """Derive a 256-bit key from password using PBKDF2."""
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    return kdf.derive(password.encode('utf-8'))


def encrypt(plaintext: str, password: str) -> str:
    """Encrypt plaintext using AES-256-GCM."""
    # Generate random salt and IV
    salt = os.urandom(16)
    iv = os.urandom(12)
    
    # Derive key from password
    key = derive_key(password, salt)
    
    # Create AESGCM cipher and encrypt
    aesgcm = AESGCM(key)
    ciphertext = aesgcm.encrypt(iv, plaintext.encode('utf-8'), None)
    
    # Combine salt + iv + ciphertext and encode to base64
    # Format: [salt(16)] [iv(12)] [ciphertext+tag(variable)]
    combined = salt + iv + ciphertext
    return base64.b64encode(combined).decode('utf-8')


def decrypt(b64_cipher: str, password: str) -> str:
    """Decrypt base64-encoded ciphertext using AES-256-GCM."""
    try:
        # Decode base64
        combined = base64.b64decode(b64_cipher)
        
        # Extract salt, iv, and ciphertext
        salt = combined[:16]
        iv = combined[16:28]
        ciphertext = combined[28:]
        
        # Derive key from password
        key = derive_key(password, salt)
        
        # Decrypt
        aesgcm = AESGCM(key)
        plaintext = aesgcm.decrypt(iv, ciphertext, None)
        
        return plaintext.decode('utf-8')
    except Exception as e:
        raise ValueError(f"Decryption failed: {str(e)}")


def main():
    if len(sys.argv) != 4:
        print("Usage: python crypto.py <encrypt|decrypt> <text> <password>")
        print("  encrypt: plaintext to encrypt")
        print("  decrypt: base64 ciphertext to decrypt")
        sys.exit(1)
    
    operation = sys.argv[1].lower()
    text = sys.argv[2]
    password = sys.argv[3]
    
    if not text:
        print("Error: Text cannot be empty")
        sys.exit(1)
    
    if not password:
        print("Error: Password cannot be empty")
        sys.exit(1)
    
    try:
        if operation == 'encrypt':
            result = encrypt(text, password)
            print(result)
        elif operation == 'decrypt':
            result = decrypt(text, password)
            print(result)
        else:
            print("Error: Operation must be 'encrypt' or 'decrypt'")
            sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
