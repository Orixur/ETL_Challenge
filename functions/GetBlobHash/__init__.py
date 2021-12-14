import logging
import hashlib
import requests

def main(url: str) -> str:
    """
        This activity will retrieve the whole blob and calculate hash
        with sha256 algorithm.

        Args:
            url (str): Url to blob
        
        Returns:
            str: Blob hash's digest
    """
    h = hashlib.sha256()
    h.update(requests.get(url).text.encode())
    return h.hexdigest()
