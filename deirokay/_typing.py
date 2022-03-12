from typing import Any, Dict, List, Union

DeirokayOption = Dict[str, Any]
DeirokayOptionsDocument = Dict[str, Union[str, Dict[str, DeirokayOption]]]

DeirokayStatement = Dict[str, Any]
DeirokayValidationItem = Dict[str, Union[str, List[DeirokayStatement]]]
DeirokayValidationDocument = Dict[str,
                                  Union[str, List[DeirokayValidationItem]]]
