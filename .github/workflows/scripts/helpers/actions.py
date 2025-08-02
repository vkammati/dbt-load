# To generate a warning annotation in the pipeline, use this function
# which will use the 'warning' workflow command
def create_warning(title: str, message: str):
    print(f"::warning title={title}::{message}")
