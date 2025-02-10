def get_sql_file(category: str, subcategory: str, use_primary: bool) -> str:
    """
    Retrieves the SQL file path based on the category, subcategory, and whether to use primary or secondary.

    :param category: The main category (e.g., 'history', 'delta').
    :param subcategory: The subcategory (e.g., 'sas', 'medfin_advantage').
    :param use_primary: Boolean to determine if the primary or secondary SQL file is required.
    :return: The file path as a string.
    """
    # Fetch the subcategory files for the given category and subcategory
    category_files = RISKRATING_SQL_FILES.get(category)
    if not category_files:
        raise ValueError(f"Invalid category '{category}' provided.")
    
    subcategory_files = category_files.get(subcategory)
    if not subcategory_files:
        raise ValueError(f"Invalid subcategory '{subcategory}' provided for category '{category}'.")
    
    # Return the appropriate file based on use_primary
    return subcategory_files["primary"] if use_primary else subcategory_files["secondary"]
