"""Build a Notes from Nature expedition."""


def get_files_missing_data():
    """Get information for files missing data."""
    return []


def copy_images(missing, dir_name):
    """Copy images to the expedition repository."""
    print(missing)
    print(dir_name)


def main():
    """The main function."""
    missing = get_files_missing_data()
    # Make directory
    copy_images(missing, '')


if __name__ == '__main__':
    main()
