import re


def clear_parentheses(channel_item):
    if bool(channel_item[channel_item.find("(") + 1 : channel_item.rfind(")")]):
        channel_item = re.sub(r"\([^()]*\)", "", channel_item)

    return channel_item.strip()


def clean_channel(channels):
    """Cleans metadata channels to convert to 'stain:target;stain:target' format
    Parameters
    ----------
    channels: str
        String of stain:target

    Returns
    -------
    combined_sorted_stains_targets: str
        Single string in stain:target;stain:target format sorted in alphebetical order by stain
    """
    stains_targets = list()
    for channel in channels.split(";"):

        # For channel entries with 'stain:target' format
        if bool(re.search(r"(:)+", channel)):
            # Split stain and target for whitespace trimming
            split_channel = channel.split(":")
            stripped_channel = [s.strip() for s in split]

            # Redefine variables and append to channel list
            stain = split_channel[0]
            target = stripped_channel[1]
            stains_targets.append(f"{stain}:{target}")

        # For channel entries with 'stain (target)' format
        elif bool(channel[channel.find("(") + 1 : channel.rfind(")")]):
            # Search for target and stain names
            target_name = channel[channel.find("(") + 1 : channel.rfind(")")]
            stain_name = re.sub(r"\([^()]*\)", "", channel)

            # Remove parentheses for stain name
            stain_name = clear_parentheses(stain_name)
            target_name = clear_parentheses(target_name)

            # Append to channel list
            stains_targets.append(f"{stain_name}:{target_name}")

    stains_targets.sort()
    combined_sorted_stains_targets = ";".join(stains_targets)

    return combined_sorted_stains_targets
