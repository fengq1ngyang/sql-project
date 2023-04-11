from pdf2image import convert_from_path
import pytesseract
from PIL import Image

# Path to the PDF file
pdf_path = 'E:\\image1\\440604202300001.pdf'

images = convert_from_path(pdf_path)


# Save each image to a file
for i, image in enumerate(images):
    image.save(f"page_{i}.jpg", "JPEG")
    # Path to the image file
    # # image_path = "path/to/image/file.jpg"
    # #
    # # # Open the image file
    # # image = Image.open(image_path)
    #
    # # Recognize text in the image
    text = pytesseract.image_to_string(image, lang='chi_sim')
    print(text.replace('\n',''))
