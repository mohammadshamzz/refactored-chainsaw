"""
Configuration validation module for trading agent.

Provides comprehensive validation for all configuration parameters
to ensure system reliability and prevent runtime errors.

Requirements: 5.3, 5.4
"""

import logging
from typing import Dict, Any, List, Tuple, Optional
import yaml
from pathlib import Path

logger = logging.getLogger(__name__)


class ConfigValidationError(Exception):
    """Raised when configuration validation fails."""
    pass


class ConfigValidator:
    """Validates trading system configuration parameters."""
    
    def __init__(self):
        """Initialize the configuration validator."""
        self.validation_rules = self._define_validation_rules()
        logger.info("ConfigValidator initialized")
    
    def _define_validation_rules(self) -> Dict[str, Dict[str, Any]]:
        """Define validation rules for all configuration parameters."""
        return {
            'trading': {
                'account_risk_percent': {
                    'type': (int, float),
                    'min': 0.1,
                    'max': 10.0,
                    'description': 'Account risk percentage per trade (0.1-10.0%)',
                    'required': True
                },
                'max_positions': {
                    'type': int,
                    'min': 1,
                    'max': 20,
                    'description': 'Maximum number of concurrent positions (1-20)',
                    'required': True
                },
                'min_dynamic_rr_ratio': {
                    'type': (int, float),
                    'min': 1.0,
                    'max': 10.0,
                    'description': 'Minimum risk-reward ratio (1.0-10.0)',
                    'required': False,
                    'default': 1.5
                },
                'use_trailing_stops': {
                    'type': bool,
                    'description': 'Enable trailing stop functionality',
                    'required': False,
                    'default': False
                },
                'trailing_stop_atr_multiplier': {
                    'type': (int, float),
                    'min': 1.0,
                    'max': 10.0,
                    'description': 'ATR multiplier for trailing stops (1.0-10.0)',
                    'required': False,
                    'default': 3.0
                },
                'atr_multiplier_for_sl': {
                    'type': (int, float),
                    'min': 1.0,
                    'max': 10.0,
                    'description': 'ATR multiplier for stop loss (1.0-10.0)',
                    'required': False,
                    'default': 2.0
                },
                'instruments': {
                    'type': list,
                    'min_length': 1,
                    'max_length': 100,
                    'description': 'List of trading instruments (1-100 symbols)',
                    'required': True
                }
            },
            'alpaca': {
                'broker_type': {
                    'type': str,
                    'allowed_values': ['alpaca'],
                    'description': 'Broker type (currently only alpaca supported)',
                    'required': True
                },
                'paper_trading': {
                    'type': bool,
                    'description': 'Enable paper trading mode',
                    'required': True
                }
            },
            'database': {
                'type': {
                    'type': str,
                    'allowed_values': ['postgresql', 'sqlite'],
                    'description': 'Database type',
                    'required': True
                },
                'host': {
                    'type': str,
                    'description': 'Database host',
                    'required': True
                },
                'port': {
                    'type': int,
                    'min': 1,
                    'max': 65535,
                    'description': 'Database port (1-65535)',
                    'required': True
                }
            },
            'backtest': {
                'initial_capital': {
                    'type': (int, float),
                    'min': 100.0,
                    'max': 10000000.0,
                    'description': 'Initial capital for backtesting ($100-$10M)',
                    'required': False,
                    'default': 100000.0
                },
                'slippage_percent': {
                    'type': (int, float),
                    'min': 0.0,
                    'max': 1.0,
                    'description': 'Slippage percentage (0.0-1.0%)',
                    'required': False,
                    'default': 0.02
                },
                'commission_per_trade': {
                    'type': (int, float),
                    'min': 0.0,
                    'max': 100.0,
                    'description': 'Commission per trade ($0-$100)',
                    'required': False,
                    'default': 0.0
                }
            }
        }
    
    def validate_config(self, config: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate complete configuration.
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []
        
        try:
            # Validate each section
            for section_name, section_rules in self.validation_rules.items():
                section_config = config.get(section_name, {})
                section_errors = self._validate_section(section_name, section_config, section_rules)
                errors.extend(section_errors)
            
            # Additional cross-section validations
            cross_validation_errors = self._validate_cross_sections(config)
            errors.extend(cross_validation_errors)
            
            is_valid = len(errors) == 0
            
            if is_valid:
                logger.info("Configuration validation passed")
            else:
                logger.error(f"Configuration validation failed with {len(errors)} errors")
                for error in errors:
                    logger.error(f"  - {error}")
            
            return is_valid, errors
            
        except Exception as e:
            error_msg = f"Configuration validation exception: {e}"
            logger.error(error_msg)
            return False, [error_msg]
    
    def _validate_section(self, section_name: str, section_config: Dict[str, Any], 
                         section_rules: Dict[str, Any]) -> List[str]:
        """Validate a configuration section."""
        errors = []
        
        for param_name, rules in section_rules.items():
            param_path = f"{section_name}.{param_name}"
            param_value = section_config.get(param_name)
            
            # Check if required parameter is missing
            if rules.get('required', False) and param_value is None:
                errors.append(f"Missing required parameter: {param_path}")
                continue
            
            # Skip validation if parameter is not provided and not required
            if param_value is None:
                continue
            
            # Validate parameter
            param_errors = self._validate_parameter(param_path, param_value, rules)
            errors.extend(param_errors)
        
        return errors
    
    def _validate_parameter(self, param_path: str, value: Any, rules: Dict[str, Any]) -> List[str]:
        """Validate a single parameter."""
        errors = []
        
        # Type validation
        expected_type = rules.get('type')
        if expected_type and not isinstance(value, expected_type):
            type_name = expected_type.__name__ if hasattr(expected_type, '__name__') else str(expected_type)
            errors.append(f"{param_path}: Expected {type_name}, got {type(value).__name__}")
            return errors  # Skip further validation if type is wrong
        
        # Numeric range validation
        if isinstance(value, (int, float)):
            min_val = rules.get('min')
            max_val = rules.get('max')
            
            try:
                if min_val is not None and value < min_val:
                    errors.append(f"{param_path}: Value {value} below minimum {min_val}")
                
                if max_val is not None and value > max_val:
                    errors.append(f"{param_path}: Value {value} above maximum {max_val}")
            except TypeError:
                # Handle comparison errors gracefully
                errors.append(f"{param_path}: Cannot compare value {value} with numeric limits")
        
        # String/List length validation
        if isinstance(value, (str, list)):
            min_length = rules.get('min_length')
            max_length = rules.get('max_length')
            
            try:
                if min_length is not None and len(value) < min_length:
                    errors.append(f"{param_path}: Length {len(value)} below minimum {min_length}")
                
                if max_length is not None and len(value) > max_length:
                    errors.append(f"{param_path}: Length {len(value)} above maximum {max_length}")
            except TypeError:
                # Handle length calculation errors gracefully
                errors.append(f"{param_path}: Cannot determine length of value {value}")
        
        # Allowed values validation
        allowed_values = rules.get('allowed_values')
        if allowed_values and value not in allowed_values:
            errors.append(f"{param_path}: Value '{value}' not in allowed values {allowed_values}")
        
        return errors
    
    def _validate_cross_sections(self, config: Dict[str, Any]) -> List[str]:
        """Validate cross-section dependencies and logic."""
        errors = []
        
        # Validate trailing stop configuration consistency
        trading_config = config.get('trading', {})
        if trading_config.get('use_trailing_stops', False):
            required_params = ['trailing_stop_atr_multiplier', 'atr_multiplier_for_sl']
            for param in required_params:
                if param not in trading_config:
                    errors.append(f"trading.{param} required when use_trailing_stops is enabled")
        
        # Validate risk management consistency
        account_risk = trading_config.get('account_risk_percent', 0)
        max_positions = trading_config.get('max_positions', 1)
        
        # Warn if total risk could exceed reasonable limits
        try:
            total_risk = account_risk * max_positions
            if total_risk > 20.0:  # More than 20% total risk
                errors.append(
                    f"Total portfolio risk ({total_risk:.1f}%) may be excessive. "
                    f"Consider reducing account_risk_percent or max_positions."
                )
        except (TypeError, ValueError):
            # Skip total risk calculation if values are invalid
            pass
        
        # Validate backtest configuration
        backtest_config = config.get('backtest', {})
        if backtest_config:
            initial_capital = backtest_config.get('initial_capital', 0)
            if initial_capital > 0:
                # Check if risk per trade is reasonable for capital
                risk_per_trade = initial_capital * (account_risk / 100)
                if risk_per_trade < 10.0:  # Less than $10 risk per trade
                    errors.append(
                        f"Risk per trade (${risk_per_trade:.2f}) may be too small for effective backtesting. "
                        f"Consider increasing initial_capital or account_risk_percent."
                    )
        
        return errors
    
    def get_parameter_documentation(self) -> str:
        """Generate documentation for all configuration parameters."""
        doc_lines = ["# Trading System Configuration Parameters\n"]
        
        for section_name, section_rules in self.validation_rules.items():
            doc_lines.append(f"## {section_name.title()} Configuration\n")
            
            for param_name, rules in section_rules.items():
                doc_lines.append(f"### {param_name}")
                doc_lines.append(f"- **Description**: {rules.get('description', 'No description available')}")
                doc_lines.append(f"- **Type**: {self._format_type(rules.get('type'))}")
                doc_lines.append(f"- **Required**: {'Yes' if rules.get('required', False) else 'No'}")
                
                if 'min' in rules or 'max' in rules:
                    range_str = f"{rules.get('min', 'N/A')} - {rules.get('max', 'N/A')}"
                    doc_lines.append(f"- **Range**: {range_str}")
                
                if 'allowed_values' in rules:
                    doc_lines.append(f"- **Allowed Values**: {rules['allowed_values']}")
                
                if 'default' in rules:
                    doc_lines.append(f"- **Default**: {rules['default']}")
                
                doc_lines.append("")  # Empty line
            
            doc_lines.append("")  # Empty line between sections
        
        return "\n".join(doc_lines)
    
    def _format_type(self, type_spec) -> str:
        """Format type specification for documentation."""
        if isinstance(type_spec, tuple):
            return " or ".join(t.__name__ for t in type_spec)
        elif hasattr(type_spec, '__name__'):
            return type_spec.__name__
        else:
            return str(type_spec)
    
    def apply_defaults(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Apply default values to configuration where parameters are missing."""
        updated_config = config.copy()
        
        for section_name, section_rules in self.validation_rules.items():
            if section_name not in updated_config:
                updated_config[section_name] = {}
            
            section_config = updated_config[section_name]
            
            for param_name, rules in section_rules.items():
                if param_name not in section_config and 'default' in rules:
                    section_config[param_name] = rules['default']
                    logger.info(f"Applied default value for {section_name}.{param_name}: {rules['default']}")
        
        return updated_config


def validate_config_file(config_path: str) -> Tuple[bool, List[str], Optional[Dict[str, Any]]]:
    """
    Validate a configuration file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Tuple of (is_valid, error_messages, config_dict)
    """
    try:
        # Load configuration file
        config_file = Path(config_path)
        if not config_file.exists():
            return False, [f"Configuration file not found: {config_path}"], None
        
        with open(config_file, 'r') as f:
            if config_path.endswith('.yaml') or config_path.endswith('.yml'):
                config = yaml.safe_load(f)
            else:
                return False, [f"Unsupported configuration file format: {config_path}"], None
        
        # Validate configuration
        validator = ConfigValidator()
        is_valid, errors = validator.validate_config(config)
        
        # Apply defaults if validation passed
        if is_valid:
            config = validator.apply_defaults(config)
        
        return is_valid, errors, config
        
    except Exception as e:
        error_msg = f"Error loading configuration file {config_path}: {e}"
        logger.error(error_msg)
        return False, [error_msg], None


def generate_config_documentation(output_path: str = "config_documentation.md"):
    """Generate configuration documentation file."""
    try:
        validator = ConfigValidator()
        documentation = validator.get_parameter_documentation()
        
        with open(output_path, 'w') as f:
            f.write(documentation)
        
        logger.info(f"Configuration documentation generated: {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error generating configuration documentation: {e}")
        return False


if __name__ == '__main__':
    # Example usage
    import sys
    
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
        is_valid, errors, config = validate_config_file(config_path)
        
        if is_valid:
            print(f"✅ Configuration file {config_path} is valid")
        else:
            print(f"❌ Configuration file {config_path} has errors:")
            for error in errors:
                print(f"  - {error}")
    else:
        # Generate documentation
        generate_config_documentation()
        print("📖 Configuration documentation generated: config_documentation.md")