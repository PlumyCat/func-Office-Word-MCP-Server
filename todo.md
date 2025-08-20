# Refactorisation à effectuer

Voici les consignes de refactorisation proposées pour ce projet :

- Découper `src/function_app.py` en plusieurs modules clairs (gestion du stockage, gestion des modèles Word, routes HTTP...).
- Introduire davantage de typage statique et d'annotations de type pour les fonctions publiques.
- Réduire l'utilisation de l'état global : encapsuler les dépendances (_blob_service_client, _docx, etc.) dans des classes ou modules dédiés.
- Documenter chaque fonction publique avec des docstrings et commentaires pertinents.
- Ajouter des tests unitaires couvrant les cas principaux et les erreurs possibles.
- Centraliser la configuration (variables d'environnement) dans un module unique afin d'éviter les accès dispersés à `os.environ`.
- Gérer les erreurs de manière uniforme via des exceptions spécialisées ou un module d'utilitaires.

Ces éléments serviront de base pour la prochaine phase de refactorisation.
