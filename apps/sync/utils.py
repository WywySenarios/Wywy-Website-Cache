

import re
from typing import List
from os import environ as env


def to_lower_snake_case(target: str) -> str:
    """Attempts to convert from regular words/sentences to lower_snake_case. This will not affect strings already in underscore notation. (Does not work with camelCase)
    @param target
    @return Returns lower_snake_case string. e.g. "hi I am Wywy" -> "hi_i_am_wywy"
    """
    stringFrags: List[str] = re.split(r"[\.\ \-]", target)
    
    output: str = ""
    
    for i in stringFrags:
        output += i.lower() + "_"
    
    return output[:-1] # remove trailing underscore with "[:-1]"

def to_snake_case(target: str) -> str:
    """Attempts to convert from regular words/sentences to snake_case. This will not affect strings already in underscore notation. (Does not work with camelCase)
    @param target
    @return Returns underscore notation string. e.g. "hi I am Wywy" -> "hi_I_am_Wywy"
    """
    stringFrags: List[str] = re.split(r"[\.\ \-]", target)
    
    output: str = ""
    
    for i in stringFrags:
        output += i + "_"
    
    return output[:-1] # remove trailing underscore with "[:-1]"

def remove_quotation(target: str) -> str:
    """Returns a copy of the target string that is not singly quoted.

    Args:
        target (str): The string to unquote.

    Returns:
        str: An unquoted copy of the target string.
    """
    return target[:-1][1:]

def chunkify_url(url: str, max_chunks: int = -1) -> List[str]:
    """Breaks a URL into chunks.

    Args:
        url (str): The URL to split
        max_chunks (int, optional): The maximum length of the list. Passing in no value or a value less than zero will put no restrictions on the number of chunks.

    Returns:
        List[str]: A list of the URL chunks, in order.
    """
    chunky_url: str = url
    # remove http:// at the start if necessary
    if url.startswith("http://"):
        chunky_url = url[7:]
    elif url.startswith("https://"):
        chunky_url = url[8:]
    elif url.startswith("/"):
        chunky_url = url[1:]

    if (max_chunks < 0):
        return chunky_url.split("/")
    else:
        return chunky_url.split("/")[:max_chunks]

def get_env_int(env_name: str, default_value: int = 0):
    """Attempts to get the integer value of an environment variable. Cannot parse negative numbers.

    Args:
        env_name (str): The name of the environment variable.
        default_value (int): The default value to return if the environment variable is invalid (i.e. not an non-negative integer) or the environment variable doesn't exist.

    Returns:
        int: The respective value.
    """
    return int(env.get(env_name, default_value)) if env.get(env_name, default_value).isdigit() else default_value