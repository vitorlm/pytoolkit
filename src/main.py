import argparse
import importlib
import os
import pkgutil

from log_config import log_manager

logger = log_manager.get_logger(__name__)


def main():
    """
    Main function to handle CLI commands with dynamic module loading.
    """
    parser = argparse.ArgumentParser(description="CLI tool for various domain commands.")
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
            domain_name = domain_name.strip()
            try:
                domain_parser = subparsers.add_parser(
                    domain_name, help=f"{domain_name} domain commands"
                )
                subdomain_subparsers = domain_parser.add_subparsers(
                    dest="subdomain", help="Available subdomains"
                )

                domain_module_path = os.path.join(domains_path, domain_name)

                for _, subdomain_name, subpkg in pkgutil.iter_modules([domain_module_path]):
                    if subpkg:
                        subdomain_name = subdomain_name.strip()
                        subdomain_parser = subdomain_subparsers.add_parser(
                            subdomain_name,
                            help=f"{subdomain_name} subdomain commands",
                        )
                        command_subparsers = subdomain_parser.add_subparsers(
                            dest="command", help="Available commands"
                        )

                        subdomain_module_path = os.path.join(domain_module_path, subdomain_name)

                        for _, module_name, _ in pkgutil.iter_modules([subdomain_module_path]):
                            module_name = module_name.strip()
                            module_path = f"domains.{domain_name}.{subdomain_name}.{module_name}"

                            try:
                                module = importlib.import_module(module_path)

                                if hasattr(module, "main"):
                                    command_parser = command_subparsers.add_parser(
                                        module_name,
                                        help=module.main.__doc__,
                                    )

                                    if hasattr(module, "get_arguments"):
                                        module.get_arguments(command_parser)

                                    command_parser.set_defaults(func=module.main)
                                else:
                                    logger.warning(
                                        f"Module '{module_path}' does not have a 'main' "
                                        "function, skipping."
                                    )
                            except ImportError as e:
                                logger.warning(
                                    f"Could not import module '{module_path}': {e}. Using fallback."
                                )
                            except Exception as e:
                                logger.error(f"Unexpected error importing '{module_path}': {e}")
            except Exception as e:
                logger.error(
                    f"Error processing domain '{domain_name}': {e}",
                    exc_info=True,
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
        logger.error(f"An error occurred while executing the command: {e}", exc_info=True)


if __name__ == "__main__":
    main()
