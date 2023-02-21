import logging
logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_least_most_significant_bits(s):
    sp = s.split("-")
    lsb_s = "".join(sp[-2:])
    lsb = int(lsb_s, 16)
    if int(lsb_s[0], 16) > 7:
        # negative
        lsb = lsb - 0x10000000000000000

    msb_s = "".join(sp[:3])
    msb = int(msb_s, 16)
    if int(msb_s[0], 16) > 7:
        # negative
        msb = msb - 0x10000000000000000
    return lsb, msb
