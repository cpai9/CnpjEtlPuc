import re

#Utiliza do regex para extrair a data-corte da base e o tipo de entidade para agrupamento posterior
def retornaReg(pNomeArq):
  try:
    regDicTipo = re.search(r'(\d{0,8})_(\d{0,4}_\D+)(.*?)(?=\.)', pNomeArq)
    regDicTipo = regDicTipo.group(2)
    return regDicTipo
  except:
    regDicTipo = re.search(r'[^_]*$', pNomeArq)
    regDicTipo = regDicTipo.group(0)
    return regDicTipo.replace(".CSV", "")
