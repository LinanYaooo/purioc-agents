---
name: purioc-simple-crypto-tool
description: Simple symmetric encryption/decryption tool using AES-256-GCM. Use when users want to encrypt or decrypt text with a password/key. Supports both encryption and decryption operations with secure AES-256-GCM algorithm. Trigger when users mention encrypt, decrypt, cipher, AES, or want to secure text with a password.
---

# Purioc Simple Crypto Tool

A simple symmetric encryption and decryption tool for securing text with a password or key.

## Features

- **Algorithm**: AES-256-GCM (industry-standard authenticated encryption)
- **Key Derivation**: PBKDF2 with 100,000 iterations
- **Input**: Plain text and user-provided password/key
- **Output**: Base64-encoded encrypted text (includes salt, IV, and authentication tag)

## Usage

### Encrypting Text

When the user wants to encrypt text:

1. Ask for the text to encrypt
2. Ask for the encryption key/password
3. Run the encryption script
4. Return the Base64-encoded result

**Example usage:**
```
User: "Encrypt this text: 'Hello World' with key 'mypassword123'"
```

### Decrypting Text

When the user wants to decrypt text:

1. Ask for the encrypted text (Base64)
2. Ask for the decryption key/password
3. Run the decryption script
4. Return the decrypted plaintext

**Example usage:**
```
User: "Decrypt this: 'AbC123XyZ...' with key 'mypassword123'"
```

## Implementation

Use the bundled Python script for encryption/decryption operations.

### Encryption

```python
# Run: python scripts/crypto.py encrypt "plaintext" "password"
```

### Decryption

```python
# Run: python scripts/crypto.py decrypt "base64cipher" "password"
```

## Security Notes

- **Never store keys**: The tool does not store or remember passwords
- **Strong passwords**: Encourage users to use strong, unique passwords
- **Authenticated encryption**: AES-256-GCM provides both confidentiality and integrity
- **Salt**: Each encryption uses a random 16-byte salt for key derivation
- **IV**: Each encryption uses a random 12-byte IV (nonce)

## Examples

**Example 1 - Encrypt:**
```
Input: Encrypt "Sensitive data here" with password "MySecretKey2024!"
Output: U2FsdGVkX1+AbCdEfGhIjKlMnOpQrStUvWxYz1234567890...
```

**Example 2 - Decrypt:**
```
Input: Decrypt "U2FsdGVkX1+..." with password "MySecretKey2024!"
Output: Sensitive data here
```

**Example 3 - File content:**
```
Input: Encrypt the content of config.txt with key "prod-secret-key"
Process: Read file → Encrypt → Return Base64 string
```

## Error Handling

Common errors to handle:
- **Wrong password**: Decryption will fail with authentication error
- **Corrupted data**: Invalid Base64 or tampered ciphertext
- **Empty input**: Handle empty strings gracefully

Always validate inputs before processing and provide clear error messages.

## Workflow

1. **Determine operation**: Encrypt or decrypt?
2. **Get inputs**: Text and password from user
3. **Validate**: Ensure both text and password are provided
4. **Execute**: Run the crypto script with appropriate arguments
5. **Return**: Show the result to the user
6. **Warn**: Remind users to safely store their password - it cannot be recovered!

## Important Warnings

⚠️ **Critical**: If the password is lost, the encrypted data cannot be recovered!

⚠️ **Security**: Use strong, unique passwords (12+ characters, mixed case, numbers, symbols)

⚠️ **Transmission**: Encrypted data is safe to transmit, but passwords should never be sent over insecure channels
