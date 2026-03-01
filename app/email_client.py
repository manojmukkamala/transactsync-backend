import email
import imaplib
import logging
from email.header import decode_header
from email.utils import parsedate_to_datetime

from bs4 import BeautifulSoup


class EmailClient:
    def __init__(
        self,
        logger: logging.Logger,
        host: str,
        port: int,
        username: str,
        password: str,
        folder: str,
    ) -> None:
        self.logger = logger
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.folder = folder

    def imap_bridge(self) -> imaplib.IMAP4:
        """
        Connect to an email account using the IMAP protocol.

        Returns:
            imaplib.IMAP4: An instance of the IMAP4 class representing the connection to the email account.
        """
        try:
            # Connect to the IMAP server
            self.logger.info('Connecting to email host: %s:%s', self.host, self.port)
            self.imapb = imaplib.IMAP4(self.host, self.port)
            # Login to the account
            self.imapb.login(self.username, self.password)
        except Exception as e:
            error_msg = f'Failed to connect to email account: {e!s}'
            raise RuntimeError(error_msg) from e
        return self.imapb

    def get_email_uids(self, last_seen_uid: str | None = None) -> list[str]:
        """
        Retrieve UIDs of emails in a folder. If last_seen_uid is given, only fetch newer ones.

        Args:
            last_seen_uid: last_seen_uid

        Returns:
            list: A list containing all email index numbers in the specified folder.
        """

        def _raise_connection_error() -> None:
            error_msg = 'IMAP connection not established. Call imap_bridge first.'
            raise RuntimeError(error_msg)

        def _raise_folder_error() -> None:
            error_msg = f'Failed to select folder: {self.folder}'
            raise RuntimeError(error_msg)

        def _raise_search_error() -> None:
            error_msg = 'Failed to search for emails'
            raise RuntimeError(error_msg)

        try:
            if not hasattr(self, 'imapb') or self.imapb is None:
                _raise_connection_error()

            status, _ = self.imapb.select(f'"{self.folder}"')
            if status != 'OK':
                _raise_folder_error()

            # Search from UID+1 to newest
            criteria = f'UID {int(last_seen_uid) + 1}:*' if last_seen_uid else 'ALL'

            status, messages = self.imapb.uid('search', criteria)
            if status != 'OK':
                _raise_search_error()

            # Ensure we return a list of strings
            uids = messages[0].split()
            return [
                uid.decode('utf-8') if isinstance(uid, bytes) else uid for uid in uids
            ]
        except Exception as e:
            error_msg = f'Failed to retrieve email UIDs: {e!s}'
            raise RuntimeError(error_msg) from e

    # Note: `get_emails` removed — use `get_email_uids` + `get_email_by_uid` for sequential processing

    def _extract_text_plain_body(self, part: email.message.Message) -> str:
        """Extract text/plain body from email part."""
        payload = part.get_payload(decode=True)
        if isinstance(payload, bytes):
            return payload.decode(errors='ignore')
        elif isinstance(payload, str):
            return payload
        return ''

    def _extract_html_body(self, part: email.message.Message) -> str:
        """Extract and parse HTML body from email part."""
        payload = part.get_payload(decode=True)
        if isinstance(payload, bytes):
            html = payload.decode(errors='ignore')
        elif isinstance(payload, str):
            html = payload
        else:
            return ''
        soup = BeautifulSoup(html, 'html.parser')
        return soup.get_text()

    def _extract_email_body(self, msg: email.message.Message) -> str:
        """Extract body from email message."""
        body = ''
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                if content_type == 'text/plain':
                    body = self._extract_text_plain_body(part)
                    break
                elif content_type == 'text/html':
                    body = self._extract_html_body(part)
                    break
        else:
            content_type = msg.get_content_type()
            if content_type == 'text/html':
                body = self._extract_html_body(msg)
            else:
                payload = msg.get_payload(decode=True)
                if isinstance(payload, bytes):
                    body = payload.decode(errors='ignore')
                elif isinstance(payload, str):
                    body = payload
        return body

    def _parse_email_date(self, raw_date: str) -> str:
        """Parse and format email date."""
        parsed_date = parsedate_to_datetime(raw_date)
        # Convert to naive datetime (remove timezone info) to match database expectations
        if parsed_date and parsed_date.tzinfo is not None:
            parsed_date = parsed_date.replace(tzinfo=None)
        return parsed_date.isoformat() if parsed_date else raw_date

    def get_email_by_uid(self, uid: str) -> dict[str, str] | None:
        """
        Retrieve a single email by UID.

        Args:
            uid: UID of the email to fetch.

        Returns:
            dict: A dictionary containing email details, or None on failure.
        """

        def _raise_folder_error() -> None:
            error_msg = f'Failed to select folder: {self.folder}'
            raise RuntimeError(error_msg)

        try:
            self.imapb = self.imap_bridge()
            # Select the folder before fetching the email
            status, _ = self.imapb.select(f'"{self.folder}"')
            if status != 'OK':
                _raise_folder_error()

            status, msg_data = self.imapb.uid('fetch', uid, '(RFC822)')
            if status != 'OK':
                self.logger.error('Failed to fetch email UID %s', uid)
                self.imapb.logout()
                return None

            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)

            # Decode subject
            subject_header = msg['Subject']
            if subject_header:
                decoded_parts = decode_header(subject_header)
                # Take the first part and decode it properly
                subject, encoding = decoded_parts[0]
                if isinstance(subject, bytes):
                    subject = subject.decode(encoding or 'utf-8')
            else:
                subject = ''

            # Decode date
            raw_date = msg['Date']
            email_date = self._parse_email_date(raw_date)

            # Decode from
            from_address = msg['From'] or ''

            # Decode to
            to_address = msg['To'] or ''

            # Extract body
            body = self._extract_email_body(msg)

            self.imapb.logout()
            # Ensure uid is a string
            uid_str = uid.decode('utf-8') if isinstance(uid, bytes) else str(uid)
            # Return statement moved to an else block to fix TRY300
            result = {
                'uid': uid_str,
                'subject': subject,
                'email_date': email_date,
                'from_address': from_address,
                'to_address': to_address,
                'body': body,
            }
        except Exception as e:
            error_msg = f'Failed to retrieve email UID {uid}: {e!s}'
            raise RuntimeError(error_msg) from e

        return result
