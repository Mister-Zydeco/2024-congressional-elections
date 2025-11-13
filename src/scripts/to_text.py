import re
import sys

import pdfplumber


def convert_to_text(fname: str) -> None:
    ptext: list[str] = []
    with pdfplumber.open(fname) as pdf:
        for page in pdf.pages:
            ptext.append(page.extract_text())
    text = '\n'.join(ptext)
    txt_fname = re.sub('.pdf$', '.txt', fname)
    with open(txt_fname, 'w') as wfh:
        wfh.write(text)


if __name__ == '__main__':
    convert_to_text(sys.argv[1])
