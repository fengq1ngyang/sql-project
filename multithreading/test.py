from pdf2image import convert_from_path
import pytesseract
from PIL import Image

path = 'img.png'

text = pytesseract.image_to_string(path, lang='chi_sim')
print(text.strip())
