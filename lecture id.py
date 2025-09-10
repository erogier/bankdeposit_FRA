
def lecture_liste_siren(fichier_siren):
    liste_siren = []
    with open(fichier_siren, 'r') as file:
        liste_siren = [line.strip() for line in file]
    with open(fichier_siren, 'r') as file:
        nb_siren = len(file.readlines())
    return liste_siren, nb_siren

def collecte_token(username, password):
    url = "https://registre-national-entreprises.inpi.fr/api/sso/login"
    headers = {"Content-Type": "application/json"}
    data = {"username": "emile.rogier@hec.edu", "password": "Hippolyte=12"}
    response = requests.post(url, json=data, headers=headers)
    if response.status_code == 200:
        return response.json()["token"]
    else:
        raise Exception(f"Échec de l'authentification. Code d'erreur : {response.status_code}")
