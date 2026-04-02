import yaml
from wywy_website_types import MainConfig

# peak at config
with open("/home/wywy/config.yml", "r") as file:
    CONFIG: MainConfig = yaml.safe_load(file)
