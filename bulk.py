import logging

import pandas as pd

from constants import gov_list
from data_validator import (extract_info_from_national_id, validate_column,
                            validate_column_unique_values, validate_gov_column,
                            collect_services)


def bulk_file(file_path, required_columns):
    try:
        invalid_item = []

        df = pd.read_csv(file_path)
        total_rows = len(df)
        for start in range(0, total_rows, 1000):
            end = min(start + 1000, total_rows)
            batch = df.iloc[start:end]

            # Validate columns
            # Terminate if invalid
            check_column = validate_column(df, required_columns)
            if not check_column:
                raise ValueError("Invalid columns in the CSV file.")

            # Validate column values
            # Terminate if invalid
            check_association_column = validate_column_unique_values(df, 'association')
            if not check_association_column:
                raise ValueError("Invalid values in the association column.")

            check_type_column = validate_column_unique_values(df, 'type')
            if not check_type_column:
                raise ValueError("Invalid values in the type column.")

            # Validate the Gov
            # Terminate if invalid
            if 'gov' in df.columns:
                check_gov = validate_gov_column(df, gov_list)
                if not check_gov:
                    raise ValueError("Invalid values in the gov column.")

            for _, row in batch.iterrows():
                document = row.to_dict()

                # Ensure that person_details is a dictionary
                person_details = extract_info_from_national_id(document['nationalID'])

                if isinstance(person_details, dict):
                    full_document = {
                        "birthdate": person_details['birthdate'],
                        "age": person_details['age'],
                        "gender": person_details['gender'],
                        "nationalID": str(int(document['nationalID'])),
                        "name": document['name'] if document['name'] else None,
                        "gov": document['gov'] if document['gov'] else None,
                        "phone": document['phone'] if document['phone'] else None,
                        "subGov": document['subGov'] if document['subGov'] else None,
                        "job": document.get('job') if document.get('job') else None,
                        "association": [document['association']],
                        "type": [document.get('type')],
                        "services": collect_services(document),
                        "csv_source": [file_path],
                        "coordJobGov": document.get('jobGov') if document.get('jobGov') else None,
                        "coordJobSubGov": document.get('jobSubGov') if document.get('jobSubGov') else None,
                        "coordJob": document.get('coordJob') if document.get('coordJob') else None,
                        "volGov": document.get('volJob') if document.get('volJob') else None,
                        "volSubGov": document.get('volSubGov') if document.get('volSubGov') else None,
                        "volJob": document.get('volJob') if document.get('volJob') else None,
                        "association_count": len([document['association']]),
                        "services_count": len(collect_services(document)),
                        "type_count": len([document.get('type')]),
                        "csv_source_count": 1,
                    }

                    insertion_doc = {
                        "_op_type": "update",
                        "_index": "visualization_new_united",
                        "_id":  str(int(document['nationalID'])),
                        "script": {
                            "source": """
                                if (params.new_association.length > 0) {
                                    if (ctx._source.association == null) {
                                        ctx._source.association = params.new_association;
                                        ctx._source.association_count = params.new_association.length;
                                    } else {
                                        for (assoc in params.new_association) {
                                            if (!ctx._source.association.contains(assoc)) {
                                                ctx._source.association.add(assoc);
                                            }
                                        }
                                        ctx._source.association_count = ctx._source.association.length;
                                    }
                                }
        
                                if (params.new_services.length > 0) {
                                    if (ctx._source.services == null) {
                                        ctx._source.services = params.new_services;
                                        ctx._source.services_count = params.new_services.length;
                                    } else {
                                        for (service in params.new_services) {
                                            boolean serviceExists = false;
                                            for (existingService in ctx._source.services) {
                                                if (existingService.service == service.service &&
                                                    existingService.association == service.association) {
                                                    serviceExists = true;
                                                    break;
                                                }
                                            }
                                            if (!serviceExists) {
                                                ctx._source.services.add(service);
                                            }
                                        }
                                        ctx._source.services_count = ctx._source.services.length;
                                    }
                                }
        
                                if (ctx._source.csv_source == null) {
                                    ctx._source.csv_source = [params.csv_source];
                                    ctx._source.csv_source_count = 1;
                                } else if (!ctx._source.csv_source.contains(params.csv_source)) {
                                    ctx._source.csv_source.add(params.csv_source);
                                    ctx._source.csv_source_count = ctx._source.csv_source.length;
                                }
        
                                if (ctx._source.type == null) {
                                    ctx._source.type = params.type;
                                    ctx._source.type_count = params.type.length;
                                } else {
                                    for (t in params.type) {
                                        if (!ctx._source.type.contains(t)) {
                                            ctx._source.type.add(t);
                                        }
                                    }
                                    ctx._source.type_count = ctx._source.type.length;
                                }
        
                                if (ctx._source.type.length > 1) {
                                    ctx._source.conflicting_type = true;
                                } else {
                                    ctx._source.conflicting_type = false;
                                }
        
                                if (params.coordJobGov != null) {
                                    ctx._source.coordJobGov = params.coordJobGov;
                                }
                                if (params.coordJobSubGov != null) {
                                    ctx._source.coordJobSubGov = params.coordJobSubGov;
                                }
                                if (params.coordJob != null) {
                                    ctx._source.coordJob = params.coordJob;
                                }
                                if (params.volGov != null) {
                                    ctx._source.volGov = params.volGov;
                                }
                                if (params.volSubGov != null) {
                                    ctx._source.volSubGov = params.volSubGov;
                                }
                                if (params.volJob != null) {
                                    ctx._source.volJob = params.volJob;
                                }
                            """,
                            "lang": "painless",
                            "params": {
                                "new_association": [document['association']],
                                "new_services": collect_services(document),
                                "csv_source": file_path,
                                "type": [document.get('type')],
                                "coordJobGov": document.get('jobGov') if document.get('jobGov') else None,
                                "coordJobSubGov": document.get('jobSubGov') if document.get('jobSubGov') else None,
                                "coordJob": document.get('coordJob') if document.get('coordJob') else None,
                                "volGov": document.get('volJob') if document.get('volJob') else None,
                                "volSubGov": document.get('volSubGov') if document.get('volSubGov') else None,
                                "volJob": document.get('volJob') if document.get('volJob') else None,
                            }
                        },
                        "upsert": full_document
                    }
                    yield insertion_doc
                else:
                    invalid_item.append(row)

        if invalid_item:
            try:
                invalid_df = pd.DataFrame(invalid_item)
                invalid_df.to_csv("invalid_items.csv", index=False)
            except Exception:
                logging.error("failed to save invalid item local")
    except Exception as e:
        logging.error(f"Failed to bulk insert data: {e}")
        raise e