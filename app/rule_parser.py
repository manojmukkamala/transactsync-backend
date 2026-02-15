import email.utils
import logging
import re
from typing import Any

import yaml

logger = logging.getLogger(__name__)


def load_rules(rules_file: str = 'examples/transaction_rules.yaml') -> dict[str, Any]:
    """Load email routing rules from YAML file."""
    try:
        with open(rules_file) as file:
            rules = yaml.safe_load(file)
    except yaml.YAMLError:
        logger.exception('Error loading rules from %s', rules_file)
        return {}
    except Exception:
        logger.exception('Error loading rules from %s', rules_file)
        return {}
    return rules or {}


def extract_email_address(sender: str) -> str:
    """Extract just the email address part from a sender string that may include display name."""
    # Use email.utils.parseaddr to properly parse the sender
    parsed = email.utils.parseaddr(sender)
    email_addr = parsed[1]  # The email address part
    return email_addr or sender


def _match_exact(subject_pattern: str, email_subject: str) -> bool:
    """Match email subject with exact pattern."""
    return subject_pattern == email_subject


def _match_startswith(subject_pattern: str, email_subject: str) -> bool:
    """Match email subject with startswith pattern."""
    return email_subject.startswith(subject_pattern)


def _match_endswith(subject_pattern: str, email_subject: str) -> bool:
    """Match email subject with endswith pattern."""
    return email_subject.endswith(subject_pattern)


def _match_contains(subject_pattern: str, email_subject: str) -> bool:
    """Match email subject with contains pattern."""
    return subject_pattern in email_subject


def _match_custom(subject_pattern: str, email_subject: str) -> bool:
    """Match email subject with custom regex pattern."""
    try:
        return bool(re.search(subject_pattern, email_subject))
    except re.error:
        logger.exception("Invalid regex pattern '%s'", subject_pattern)
        return False


def _match_pattern(pattern_type: str, subject_pattern: str, email_subject: str) -> bool:
    """Match email subject against a pattern."""
    match_functions = {
        'exact': _match_exact,
        'startswith': _match_startswith,
        'endswith': _match_endswith,
        'contains': _match_contains,
        'custom': _match_custom,
    }

    match_func = match_functions.get(pattern_type, _match_exact)
    return match_func(subject_pattern, email_subject)


def match_email_to_rule(
    email_subject: str, email_sender: str, rules: dict[str, Any]
) -> dict[str, Any] | None:
    """Match email to a rule based on sender and subject."""
    senders = rules.get('senders', [])

    # Extract just the email address from the full sender string
    sender_email = extract_email_address(email_sender)

    for sender_rule in senders:
        if sender_rule.get('from_address') != sender_email:
            continue

        rules_list = sender_rule.get('rules', [])
        for rule in rules_list:
            # Check if rule is enabled (default to True if not specified)
            if not rule.get('enabled', True):
                continue

            subject_pattern = rule.get('subject', '')
            pattern_type = rule.get('pattern', 'exact')  # Default to exact matching

            # Match based on pattern type
            if _match_pattern(pattern_type, subject_pattern, email_subject):
                # Explicitly cast to make mypy happy
                return rule  # type: ignore[no-any-return]

    return None


def get_target_folder(
    email_subject: str, email_sender: str, rules: dict[str, Any]
) -> str | None:
    """Get target folder for an email based on rules."""
    matched_rule = match_email_to_rule(email_subject, email_sender, rules)
    if matched_rule is None:
        return None
    target = matched_rule.get('target')
    return target
