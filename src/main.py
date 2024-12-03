import argparse
import importlib
import logging
import os
import pkgutil
import sys

# Ensure `src` is in sys.path to enable imports
current_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.abspath(os.path.join(current_dir, "src"))

# Insert `src` at the start of sys.path, giving it priority in the import search
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """
    Main function to handle CLI commands.
    """
    parser = argparse.ArgumentParser(
        description="CLI tool for various domain commands."
    )
    subparsers = parser.add_subparsers(dest="domain", help="Available domains")

    # Path to the domains directory
    domains_path = os.path.join("src", "domains")

    # Check if domains path exists
    if not os.path.exists(domains_path):
        logger.error(
            f"Domains path '{domains_path}' does not exist. Please check the directory structure."
        )
        return

    # Dynamically load domains and their subdomains/commands
    for _, domain_name, ispkg in pkgutil.iter_modules([domains_path]):
        if ispkg:
            domain_name = domain_name.strip()  # Clean domain name
            domain_parser = subparsers.add_parser(
                domain_name, help=f"{domain_name} domain commands"
            )
            subdomain_subparsers = domain_parser.add_subparsers(
                dest="subdomain", help="Available subdomains"
            )

            # Load subdomains within the domain
            domain_module_path = os.path.join(domains_path, domain_name)
            for _, subdomain_name, subpkg in pkgutil.iter_modules([domain_module_path]):
                if subpkg:
                    subdomain_name = subdomain_name.strip()  # Clean subdomain name
                    subdomain_parser = subdomain_subparsers.add_parser(
                        subdomain_name, help=f"{subdomain_name} subdomain commands"
                    )
                    command_subparsers = subdomain_parser.add_subparsers(
                        dest="command", help="Available commands"
                    )

                    subdomain_module_path = os.path.join(
                        domain_module_path, subdomain_name
                    )

                    # Load commands within the subdomain
                    for _, module_name, _ in pkgutil.iter_modules(
                        [subdomain_module_path]
                    ):
                        module_name = module_name.strip()  # Clean module name
                        module_path = (
                            f"domains.{domain_name}.{subdomain_name}.{module_name}"
                        )

                        try:
                            # Import the module dynamically
                            module = importlib.import_module(module_path)

                            # Check if the module has a main function
                            if hasattr(module, "main"):
                                command_parser = command_subparsers.add_parser(
                                    module_name, help=module.main.__doc__
                                )

                                # Add arguments if the module provides a 'get_arguments' function
                                if hasattr(module, "get_arguments"):
                                    module.get_arguments(command_parser)

                                command_parser.set_defaults(func=module.main)
                            else:
                                logger.info(
                                    f"Ignoring module '{module_path}' as it does not have a 'main' function."
                                )
                        except ImportError as e:
                            logger.error(
                                f"Could not import module '{module_path}': {e}"
                            )
                        except Exception as e:
                            logger.error(
                                f"Unexpected error importing '{module_path}': {e}"
                            )

    # Parse arguments
    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        return

    # Execute the selected command with its arguments
    try:
        args.func(args)
    except Exception as e:
        logger.error(f"An error occurred while executing the command: {e}")


if __name__ == "__main__":
    main()
