from .chain import factory_outputs as chain_factory_outputs

factory_outputs_list = [
    chain_factory_outputs,
]

factory_outputs = sum(factory_outputs_list)
