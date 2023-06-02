import time
import requests
import csv
import asyncio
import aiohttp
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# GraphQL query to get entities
entities_query = '''
{
  entities(
    scope: "API"
    limit: 3000
    between: {
      startTime: "2023-06-01T00:00:00.273Z"
      endTime: "2023-06-01T23:59:00.273Z"
    }
    offset: 0
    filterBy: [
      {
        keyExpression: { key: "apiDiscoveryState" }
        operator: IN
        value: ["DISCOVERED"]
        type: ATTRIBUTE
      }
      {
        keyExpression: { key: "dataTypeIds" }
        operator: NOT_EQUALS
        value: "null"
        type: ATTRIBUTE
      }
      {
        keyExpression: { key: "environment" }
        operator: EQUALS
        value: "sandbox"
        type: ATTRIBUTE
      }
    ]
    includeInactive: false
  ) {
    results {
      id
      name: attribute(expression: { key: "name" })
      serviceId: attribute(expression: { key: "serviceId" })
      dataTypeIds: attribute(expression: { key: "dataTypeIds" })
      serviceName: attribute(expression: { key: "serviceName" })
      isAuthenticated: attribute(expression: { key: "isAuthenticated" })
      apiRiskScore: attribute(expression: { key: "apiRiskScore" })
      piiTypes: attribute(expression: { key: "piiTypes" })
      isLearnt: attribute(expression: { key: "isLearnt" })
      apiRiskScoreCategory: attribute(expression: { key: "apiRiskScoreCategory" })
      riskLikelihoodFactors: attribute(expression: { key: "riskLikelihoodFactors" })
      riskImpactFactors: attribute(expression: { key: "riskImpactFactors" })
    }
  }
}
'''

# GraphQL query to get details by ID
details_query = '''
query GetApiDetails($id: String!) {
  apiInsightByApiId(id: $id) {
    id
    requestSensitiveParams {
      results {
        dataTypeId
        dataSetIds
        type
        __typename
      }
      total
      __typename
    }
  }
}
'''

# Specify the Authorization token
authorization_token = ''

# Specify the URL of the GraphQL endpoint
graphql_endpoint = 'https://api.traceable.ai/graphql'

# Prepare headers with the Authorization token
headers = {
    'Authorization': f'{authorization_token}',
    'Content-Type': 'application/json',
}

# Measure the execution time
start_time = time.time()

# Chunk size for processing multiple entities in each iteration
chunk_size = 500


# Function to get all the API endpoints along with the sensitive data IDs
async def export_first_query_data_async(entities):
    data_list = []
    for entity in entities:
        data = {
            'id': entity['id'],
            'name': entity['name'],
            'dataTypeIds': entity['dataTypeIds'],
            'serviceName': entity['serviceName'],
            'isAuthenticated': entity['isAuthenticated'],
            'isLearnt': entity['isLearnt'],
            'apiRiskScoreCategory': entity['apiRiskScoreCategory'],
            'riskLikelihoodFactors': entity['riskLikelihoodFactors'],
            'riskImpactFactors': entity['riskImpactFactors']
        }
        data_list.append(data)
    return data_list


# Function to get details by ID
async def export_second_query_data_async(session, entity_ids):
    data_list = []
    tasks = []
    for entity_id in entity_ids:
        variables = {'id': entity_id}
        tasks.append(session.post(graphql_endpoint, json={'query': details_query, 'variables': variables},
                                  headers=headers))
    responses = await asyncio.gather(*tasks)
    for response in responses:
        if response.status == 200:
            data = await response.json()
            api_insight = data['data']['apiInsightByApiId']

            for param in api_insight['requestSensitiveParams']['results']:
                data_type_id = param['dataTypeId']
                data = {
                    'id': api_insight['id'],
                    'dataTypeId': data_type_id,
                    'dataSetIds': param['dataSetIds'],
                    'type': param['type'],
                    'typename': param['__typename']
                }
                data_list.append(data)

            if 'responseSensitiveParams' in api_insight:
                for param in api_insight['responseSensitiveParams']['results']:
                    data_type_id = param['dataTypeId']
                    data = {
                        'id': api_insight['id'],
                        'dataTypeId': data_type_id,
                        'dataSetIds': param['dataSetIds'],
                        'type': param['type'],
                        'typename': param['__typename']
                    }
                    data_list.append(data)

    return data_list


# Main function
async def main():
    async with aiohttp.ClientSession() as session:
        # Execute GraphQL query to get entities
        async with session.post(graphql_endpoint, json={'query': entities_query}, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                entities = data['data']['entities']['results']
                entity_ids = [entity['id'] for entity in entities]

                total_entities = len(entities)
                start_index = 0

                while start_index < total_entities:
                    end_index = start_index + chunk_size
                    batch_entities = entities[start_index:end_index]
                    record_number = start_index + 1

                    # Export data from the first query
                    first_query_data = await export_first_query_data_async(batch_entities)

                    # Export data from the second query
                    second_query_data = await export_second_query_data_async(session, entity_ids[start_index:end_index])

                    # Log the record being processed
                    logging.info(f"Processing records from {start_index} to {end_index}")

                    # Combine both lists based on "id"
                    combined_data = []
                    for entity_data in first_query_data:
                        entity_id = entity_data['id']
                        for param_data in second_query_data:
                            if param_data['id'] == entity_id:
                                combined_data.append({
                                    'id': entity_id,
                                    'name': entity_data['name'],
                                    'dataTypeIds': entity_data['dataTypeIds'],
                                    'datasetIds': param_data['dataSetIds'],
                                    'serviceName': entity_data['serviceName'],
                                    'isAuthenticated': entity_data['isAuthenticated'],
                                    'isLearnt': entity_data['isLearnt'],
                                    'apiRiskScoreCategory': entity_data['apiRiskScoreCategory'],
                                    'riskLikelihood': entity_data['riskLikelihoodFactors'],
                                    'riskImpactFactors': entity_data['riskImpactFactors'],
                                    'type': param_data['type'],
                                    'typename': param_data['typename']
                                })

                                # Log the record being processed
                                logging.info(f"Processing record for : {entity_id} : {entity_data['name']}")

                    # Introduce a wait time for demonstration purposes
                    await asyncio.sleep(0.1)

                    # Save the combined data to a CSV file
                    save_to_csv(combined_data)

                    start_index += chunk_size
        logging.info(f"Total Records Processed: {total_entities}")
        logging.info(f'Execution time: {time.time() - start_time} seconds')


# Function to save data to a CSV file
def save_to_csv(data):
    with open('combined_data.csv', 'a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        if file.tell() == 0:
            writer.writeheader()
        writer.writerows(data)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
