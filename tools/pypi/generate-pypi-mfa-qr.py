import qrcode
import subprocess
from io import BytesIO
from PIL import Image
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

vault_url = "https://mmlspark-keys.vault.azure.net"
secret_name = "pypi-mfa-uri"


def retrieve_secret_from_keyvault(vault_url, secret_name):
    try:
        return (
            SecretClient(vault_url=vault_url, credential=DefaultAzureCredential())
            .get_secret(secret_name)
            .value
        )
    except Exception as e:
        raise Exception(
            "Failed to retrieve the secret from Azure Key Vault. Ensure you are logged in with an appropiate account with 'az login'",
            e,
        )


def generate_qr_code(text):
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )
    qr.add_data(text)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color="black", back_color="white")
    return qr_img


def main():
    keyvault_secret_value = retrieve_secret_from_keyvault(vault_url, secret_name)

    if keyvault_secret_value is not None:
        qr_img = generate_qr_code(keyvault_secret_value)
        qr_img.show()


if __name__ == "__main__":
    main()
