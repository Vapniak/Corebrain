from corebrain import init

api_key = "sk_NPNLbEAjxQm86u6OX97An5ev"
#config_id = "c9913a04-a530-4ae3-a877-8e295be87f78" # MONGODB
config_id = "be43981c-0015-4ba4-9861-f12e82f6805e" # POSTGRES

# Initialize the SDK with API key and configuration ID
corebrain = init(
    api_key=api_key,
    config_id=config_id
)

"""
Corebrain possible arguments (all optionals):

- execute_query (bool)
- explain_results (bool)
- detail_level (string = "full")
"""

result = corebrain.ask("Devuélveme 5 datos interesantes sobre mis usuarios", detail_level="full")

print(result['explanation'])
