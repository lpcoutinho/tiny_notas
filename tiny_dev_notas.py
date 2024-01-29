# !pip install gspread pandas requests oauth2client

import json
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from time import sleep

import gspread
import pandas as pd
import requests
from dotenv import load_dotenv

# from google.colab import userdata
from oauth2client.service_account import ServiceAccountCredentials

load_dotenv()

# Tiny
token = os.getenv("TINY_TOKEN")
# token = userdata.get("TINY_TOKEN") # Colab
url = "https://api.tiny.com.br/api2/nota.fiscal.incluir.php"
url_transmitir_nota = 'https://api.tiny.com.br/api2/nota.fiscal.emitir.php'

# Diretório contendo os arquivos XML
# diretorio = "/content/drive/MyDrive/NF venda full" #colab
diretorio = "NF venda full/"

# Crendencias Google Sheets
credentials_path = 'google.json'
google_sheet = os.getenv("GOOGLE_SHEET")
# credentials_path = f'{diretorio}/google.json' # Colab
# google_sheet = userdata.get("GOOGLE_SHEET") # Colab

# Obter dados das notas no google sheets
credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_path)
gc = gspread.authorize(credentials)

planilha = gc.open_by_url(google_sheet)

folha = planilha.get_worksheet(0)
dados = folha.get_all_values()
df_google = pd.DataFrame(dados[1:], columns=dados[0])

# print(df_google)


def extrair_texto(valor):
    match = re.search(r"_(.*?)\-procNFe", valor)
    if match:
        return match.group(1)
    else:
        return None

def processar_xml(xml_file_path):
    namespace = {"nfe": "http://www.portalfiscal.inf.br/nfe"}
    tree = ET.parse(xml_file_path)
    root = tree.getroot()

    # Obtendo dados das primeiras linhas
    infNFe_element = root.find(".//nfe:infNFe", namespace)
    infNFe = infNFe_element.attrib.get("Id").split("NFe")[1]

    # Extrair refNFe
    refNFe_element = root.find(".//nfe:refNFe", namespace)
    refNFe = refNFe_element.text if refNFe_element is not None else None

    # Extrair dados da ide
    ide_element = root.find(".//nfe:ide", namespace)
    dados_ide = {}
    if ide_element is not None:
        dados_ide["nNF"] = ide_element.findtext(".//nfe:nNF", namespaces=namespace)
        dados_ide["dhEmi"] = ide_element.findtext(".//nfe:dhEmi", namespaces=namespace)

    # Extrair dados do destinatario
    dest_element = root.find(".//nfe:dest", namespace)
    enderDest_element = dest_element.find(".//nfe:enderDest", namespace)
    dados_dest = {}
    if dest_element is not None:
        cpf_element = dest_element.find(".//nfe:CPF", namespaces=namespace)
        if cpf_element is not None:
            dados_dest["CPF"] = cpf_element.text
        else:
            # Elemento CPF não encontrado, tentar extrair texto do elemento CNPJ
            cnpj_element = dest_element.find(".//nfe:CNPJ", namespaces=namespace)
            if cnpj_element is not None:
                dados_dest["CPF"] = cnpj_element.text
            else:
                # Ambos CPF e CNPJ não foram encontrados
                dados_dest["CPF"] = None
        dados_dest["xNome"] = dest_element.findtext(
            ".//nfe:xNome", namespaces=namespace
        )
        dados_dest["natOp"] = enderDest_element.findtext(
            ".//nfe:natOp", namespaces=namespace
        )
        dados_dest["xLgr"] = enderDest_element.findtext(
            ".//nfe:xLgr", namespaces=namespace
        )
        dados_dest["nro"] = enderDest_element.findtext(
            ".//nfe:nro", namespaces=namespace
        )
        dados_dest["xCpl"] = enderDest_element.findtext(
            ".//nfe:xCpl", namespaces=namespace
        )
        dados_dest["xBairro"] = enderDest_element.findtext(
            ".//nfe:xBairro", namespaces=namespace
        )
        dados_dest["cMun"] = enderDest_element.findtext(
            ".//nfe:cMun", namespaces=namespace
        )
        dados_dest["xMun"] = enderDest_element.findtext(
            ".//nfe:xMun", namespaces=namespace
        )
        dados_dest["UF"] = enderDest_element.findtext(".//nfe:UF", namespaces=namespace)
        dados_dest["CEP"] = enderDest_element.findtext(
            ".//nfe:CEP", namespaces=namespace
        )
        dados_dest["cPais"] = enderDest_element.findtext(
            ".//nfe:cPais", namespaces=namespace
        )
        dados_dest["xPais"] = enderDest_element.findtext(
            ".//nfe:xPais", namespaces=namespace
        )

    # Extrair dados dos itens da nota
    itens = []
    for det_element in infNFe_element.findall(".//nfe:det", namespace):
        dados_item = {}
        prod_element = det_element.find(".//nfe:prod", namespace)
        imposto_element = det_element.find(".//nfe:imposto", namespace)

        # Extrair o texto do elemento 'cProd'
        cProd_element = prod_element.find(".//nfe:cProd", namespace)
        if cProd_element is not None:
            dados_item["cProd"] = cProd_element.text
        else:
            dados_item["cProd"] = None

        # Extrair o texto do elemento 'xProd'
        xProd_element = prod_element.find(".//nfe:xProd", namespace)
        if xProd_element is not None:
            dados_item["xProd"] = xProd_element.text
        else:
            dados_item["xProd"] = None

        # Extrair o texto do elemento 'xProd'
        uCom_element = prod_element.find(".//nfe:uCom", namespace)
        if uCom_element is not None:
            dados_item["uCom"] = uCom_element.text
        else:
            dados_item["uCom"] = None

        # Extrair o texto do elemento 'qCom'
        qCom_element = prod_element.find(".//nfe:qCom", namespace)
        if qCom_element is not None:
            dados_item["qCom"] = qCom_element.text
        else:
            dados_item["qCom"] = None

        # Extrair o texto do elemento 'vUnCom'
        vUnCom_element = prod_element.find(".//nfe:vUnCom", namespace)
        if vUnCom_element is not None:
            dados_item["vUnCom"] = vUnCom_element.text
        else:
            dados_item["vUnCom"] = None

        # Extrair o texto do elemento 'orig'
        orig_element = imposto_element.find(".//nfe:orig", namespace)
        if orig_element is not None:
            dados_item["orig"] = orig_element.text
        else:
            dados_item["orig"] = None

        # Extrair o texto do elemento 'NCM'
        NCM_element = prod_element.find(".//nfe:NCM", namespace)
        if NCM_element is not None:
            dados_item["NCM"] = NCM_element.text
        else:
            dados_item["NCM"] = None

        itens.append(dados_item)

    # Extrair dados do transporte
    transp_element = root.find(".//nfe:transp", namespace)
    dados_transporte = {}
    if transp_element is not None:
        dados_transporte["qVol"] = transp_element.findtext(
            ".//nfe:qVol", namespaces=namespace
        )
        dados_transporte["pesoB"] = transp_element.findtext(
            ".//nfe:pesoB", namespaces=namespace
        )
        dados_transporte["pesoL"] = transp_element.findtext(
            ".//nfe:pesoL", namespaces=namespace
        )

    # Extrair dados de pagamento
    pag_element = root.find(".//nfe:pag", namespace)
    dados_pagamento = {}
    if pag_element is not None:
        pass

    # Extrair dados da intermediação
    infIntermed_element = root.find(".//nfe:infIntermed", namespace)
    dados_intermediacao = {}
    if infIntermed_element is not None:
        dados_intermediacao["CNPJ"] = infIntermed_element.findtext(
            ".//nfe:CNPJ", namespaces=namespace
        )
        dados_intermediacao["idCadIntTran"] = infIntermed_element.findtext(
            ".//nfe:idCadIntTran", namespaces=namespace
        )

    # Extrair dados adicionais
    infAdic_element = root.find(".//nfe:infAdic", namespace)
    dados_adicionais = {}
    if infAdic_element is not None:
        dados_adicionais["infAdFisco"] = infAdic_element.findtext(
            ".//nfe:infAdFisco", namespaces=namespace
        )
        dados_adicionais["infCpl"] = infAdic_element.findtext(
            ".//nfe:infCpl", namespaces=namespace
        )
        obsCont_element = infAdic_element.find(".//nfe:obsCont", namespace)
        if obsCont_element is not None:
            dados_adicionais["external_id"] = obsCont_element.findtext(
                ".//nfe:xTexto", namespaces=namespace
            )

    # Retornar todos os dados em um dicionário
    return {
        "infNFe": infNFe,
        "ide": dados_ide,
        "dest": dados_dest,
        "items": itens,
        "transporte": dados_transporte,
        # 'pagamento': dados_pagamento,
        # 'intermediacao': dados_intermediacao,
        # 'adicionais': dados_adicionais,
    }

def preencher_nota_json(dados_nota, df_google):
    hora_atual = datetime.now().strftime("%H:%M")

    # Formatar data de emissão
    dhEmi_datetime = datetime.strptime(
        dados_nota["ide"]["dhEmi"], "%Y-%m-%dT%H:%M:%S%z"
    )
    dhEmi = dhEmi_datetime.strftime("%d/%m/%Y")

    # Encontrar as linhas no DataFrame df_google onde o valor da coluna 'Extracao' corresponde a dados_nota['infNFe']
    linhas_correspondentes = df_google[df_google["nNF"] == dados_nota['ide']['nNF']]

    # Verificar se há linhas correspondentes
    if not linhas_correspondentes.empty:
        data_devolucao = linhas_correspondentes.iloc[0]["Data Devolução"]
    else:
        # Se não houver linhas correspondentes, definir data_emissao e data_entrada_saida como None ou qualquer outro valor padrão
        data_devolucao = None

    nota = {
        "nota_fiscal": {
            "tipo": "E",
            "id_natureza_operacao": "789259147",
            "natureza_operacao": "Devolução mercadorias",
            "data_emissao": data_devolucao,  # Data definida pelo usuário ou encontrada no sheets
            "data_entrada_saida": data_devolucao,  # Data definida pelo usuário ou encontrada no sheets
            "hora_entrada_saida": hora_atual,
            "cliente": {
                "nome": dados_nota["dest"]["xNome"],
                "tipo_pessoa": "F",
                "cpf_cnpj": dados_nota["dest"]["CPF"],
                "endereco": dados_nota["dest"]["xLgr"],
                "numero": dados_nota["dest"]["nro"],
                "complemento": dados_nota["dest"]["xCpl"],
                "bairro": dados_nota["dest"]["xBairro"],
                "cep": dados_nota["dest"]["CEP"],
                "cidade": dados_nota["dest"]["xMun"],
                "uf": dados_nota["dest"]["UF"],
                "atualizar_cliente": "N",
            },
            "itens": [],
            "meio_pagamento": "90",
            "frete_por_conta": "D",
            "quantidade_volumes": dados_nota["transporte"]["qVol"],
            "obs": f"Número da NF-e referenciada: {dados_nota['ide']['nNF']} Data de emissão da NF-e referenciada: {dhEmi} Chave de acesso da NF-e referenciada: {dados_nota['infNFe']}",
            "finalidade": "4",
            "refNFe": dados_nota["infNFe"],
        }
    }

    # Preencher itens
    for item in dados_nota["items"]:
        novo_item = {
            "item": {
                "codigo": item["cProd"],
                "descricao": item.get(
                    "xProd", ""
                ),  # Se 'xProd' não estiver presente, retorna uma string vazia
                "unidade": item["uCom"],
                "quantidade": item["qCom"],
                "valor_unitario": item["vUnCom"],
                "tipo": "P",
                "origem": item["orig"],
                "ncm": item["NCM"],
                "peso_bruto": dados_nota["transporte"]["pesoB"],
                "peso_liquido": dados_nota["transporte"]["pesoL"],
            }
        }
        nota["nota_fiscal"]["itens"].append(novo_item)

    return nota, data_devolucao

def enviar_REST(url, data):
    try:
        response = requests.post(url, data=data)
        response.raise_for_status()  # Lança uma exceção para erros HTTP
        return response.text
    except requests.exceptions.RequestException as e:
        raise Exception(f"Problema com {url}, {str(e)}")

def extrair_resultado(resultado):
    resultado_obj = json.loads(resultado)

    if resultado_obj['retorno']['status'] == 'OK':
        id_valor = resultado_obj['retorno']['registros']['registro']['id']
        serie_valor = resultado_obj['retorno']['registros']['registro']['serie']
        numero_valor = resultado_obj['retorno']['registros']['registro']['numero']

        print("Valores armazenados:")
        print(f"ID: {id_valor}")
        print(f"Série: {serie_valor}")
        print(f"Número: {numero_valor}")

        return id_valor, serie_valor, numero_valor
    else:
        print("O status não está OK. Nota não passou.")
        return None, None, None


resultado_list = []
notas_list = []
resultados_transmitir = []
counter = 0

try:
    select_date = input("Para enviar todas as notas aperte Enter. Se quer enviar notas por data digite uma data no formato dd/mm/aaaa: \n")

    if len(select_date) > 0:
        # Verificar se a data inserida pelo usuário está no formato correto (dd/mm/aaaa)
        datetime.strptime(select_date, "%d/%m/%Y")
        print(f"\n Serão enviadas notas da seguinte data: {select_date}\n")
    else:
        print(f"\n Todas as notas serão enviadas\n")

    # Iterar sobre os arquivos no diretório
    for filename in os.listdir(diretorio):
        if filename.endswith(".xml"):
            # Construir o caminho completo do arquivo XML
            try:
                xml_file_path = os.path.join(diretorio, filename)
            except:
                xml_file_path = os.path.join(filename)
            # print(xml_file_path)

            # Processar o arquivo XML e obter os dados da nota
            dados_nota = processar_xml(xml_file_path)
            # print(dados_nota)

            # Preencher a nota no formato JSON com os dados extraídos
            nota_preenchida_json, data_devolucao  = preencher_nota_json(dados_nota,df_google)
            nota_preenchida_json = json.dumps(
                nota_preenchida_json, indent=4, ensure_ascii=False
            )
            
            # print(nota_preenchida_json)
            
            data = {"token": token, "nota": nota_preenchida_json, "formato": "JSON"}
            # print(data)
            
            try:
                if select_date == data_devolucao:
                    print(f"Data selecionada: {select_date}")
                    counter += 1

                    if counter % 30 == 0:
                        print("Aguardando 1 minuto...")
                        sleep(60)

                    print(f'### Loop nº: {counter} ###')
                    print(nota_preenchida_json)
                    notas_list.append(nota_preenchida_json)
                    resultado = enviar_REST(url, data)
                    resultado_list.append(resultado)
                    # print(resultado)
                    
                    id_valor, serie_valor, numero_valor = extrair_resultado(resultado)
                    data_transmitir = {"token": token, "id": id_valor, "serie": serie_valor, "numero": numero_valor, "formato": "JSON"}
                    resultado_transmitir = enviar_REST(url_transmitir_nota, data_transmitir)
                    resultados_transmitir.append(resultado_transmitir)
                    
                elif len(select_date) == 0:
                    if data_devolucao != None:
                        print(f"Data selecionada: {select_date}")
                        # print(type(data_devolucao))
                        counter += 1
                        if counter % 30 == 0:
                            print("Aguardando 1 minuto...")
                            sleep(60)

                        print(f'### Loop nº: {counter} ###')
                        # print(nota_preenchida_json)
                        notas_list.append(nota_preenchida_json)
                        resultado = enviar_REST(url, data)
                        resultado_list.append(resultado)
                        # print(resultado)
                        
                        id_valor, serie_valor, numero_valor = extrair_resultado(resultado)
                        data_transmitir = {"token": token, "id": id_valor, "serie": serie_valor, "numero": numero_valor, "formato": "JSON"}
                        resultado_transmitir = enviar_REST(url_transmitir_nota, data_transmitir)
                        resultados_transmitir.append(resultado_transmitir)             
                    else:
                        print(f'Nada acontece, data = {select_date}')
                else:
                    pass
            except Exception as e:
                print(f"Erro: {str(e)}")
                print(f"ATENÇÃO!!! NENHUMA NOTA ENVIADA! Verifique a data informada {select_date}")

except ValueError:
    print("\nErro: Data inserida em formato incorreto. Certifique-se de inserir no formato dd/mm/aaaa.\n")
except Exception as e:
    print(f"\nErro: {str(e)}\n")

# print(resultado_list)
# print(resultados_transmitir)