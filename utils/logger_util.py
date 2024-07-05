import logging


def get_module_logger(mod_name, package_name="FLIGHT_RADAR"):
    """Fonction de parametrage des logs.
        Initialise un logger selon une certaine convention.
        date -package name - type of log - msg

    Args:
        mod_name (String): Nom du module
        package_name (str, optional): _description_. Defaults to "PYTHONTOOLBOX".
        
    Returns:
        logger (logging.logger): Objet logger afin d'afficher une information
        
    Examples
    --------
    >>> logger = get_module_logger("my package")
    >>> logger.info("This is a information")
    Date heure - my package - INFO - This is a information
    """
    
    if package_name is None:

        log_info = mod_name
    else:

        log_info = package_name + " - " + mod_name

    logger = logging.getLogger(log_info)
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
