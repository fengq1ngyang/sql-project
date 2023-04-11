import cn2an
import re

text = '地上3、4层，地下1层'
text = re.findall(r"^地上(\S+)层",text)
print(text)
# output = cn2an.cn2an(text.group(1), "smart")
print()
