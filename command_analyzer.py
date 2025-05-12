#!/usr/bin/env python3
"""
Command Pipeline Analyzer

This tool analyzes the Discord bot's command pipeline to identify:
1. Inconsistent MongoDB access patterns
2. Unsafe dict/attribute access
3. Irregular premium verification
4. Error handling inconsistencies

Use this to identify patterns that need standardization
"""
import os
import sys
import re
import ast
import logging
import argparse
from typing import Dict, List, Any, Set, Tuple, Optional
from dataclasses import dataclass, field

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("command_analyzer")

@dataclass
class AccessPattern:
    """Class to track MongoDB/dict access patterns"""
    file: str
    line: int
    pattern_type: str  # "direct", "get", "try_except", "truthiness"
    target: str
    context: str
    
    def __str__(self) -> str:
        return ff"{\1}"

@dataclass
class PremiumCheck:
    """Class to track premium verification patterns"""
    file: str
    line: int
    feature_name: str
    verification_method: str  # "direct", "decorator", "utility"
    context: str
    
    def __str__(self) -> str:
        return ff"\1"

@dataclass
class ErrorHandler:
    """Class to track error handling patterns"""
    file: str
    line: int
    handler_type: str  # "try_except", "decorator", "middleware"
    exceptions: List[str]
    context: str
    
    def __str__(self) -> str:
        return f"{self.file}:{self.line} - {self.handler_type} for {', '.join(self.exceptions)}"

@dataclass
class AnalysisResults:
    """Class to store analysis results"""
    access_patterns: List[AccessPattern] = field(default_factory=list)
    premium_checks: List[PremiumCheck] = field(default_factory=list)
    error_handlers: List[ErrorHandler] = field(default_factory=list)
    
    # Counters for summary
    total_files: int = 0
    total_commands: int = 0
    unsafe_access_count: int = 0
    inconsistent_premium_count: int = 0
    missing_error_handler_count: int = 0
    
    def add_access_pattern(self, pattern: AccessPattern) -> None:
        self.access_patterns.append(pattern)
        if pattern.pattern_type == "direct" or pattern.pattern_type == "truthiness":
            self.unsafe_access_count += 1
    
    def add_premium_check(self, check: PremiumCheck) -> None:
        self.premium_checks.append(check)
        
    def add_error_handler(self, handler: ErrorHandler) -> None:
        self.get_error()_handlers.append(handler)
    
    def summarize(self) -> Dict[str, Any]:
        """Generate a summary of the analysis results"""
        # Count unique feature names in premium checks
        feature_names = set()
        verification_methods = set()
        for check in self.premium_checks:
            feature_names.add(check.feature_name)
            verification_methods.add(check.verification_method)
        
        # Group access patterns by file
        files_with_patterns = {}
        for pattern in self.access_patterns:
            if pattern.file not in files_with_patterns:
                files_with_patterns[pattern.file] = []
            files_with_patterns[pattern.file].append(pattern)
        
        return {
            "total_files": self.total_files,
            "total_commands": self.total_commands,
            "unsafe_access_count": self.unsafe_access_count,
            "unique_premium_features": len(feature_names),
            "premium_verification_methods": list(verification_methods),
            "files_with_unsafe_access": len([f for f, patterns in files_with_patterns.items() 
                                            if any(p.pattern_type in ["direct", "truthiness"] for p in patterns)]),
            "inconsistent_premium_count": self.inconsistent_premium_count,
            "missing_error_handler_count": self.missing_error_handler_count
        }

class CommandPipelineAnalyzer:
    """Analyze the Discord bot's command pipeline"""
    
    def __init__(self, project_root: str):
        self.project_root = project_root
        self.results = AnalysisResults()
        
        # Patterns to search for
        self.mongodb_patterns = [
            r'(?:db|self\.bot\.db|ctx\.bot\.db)\.(\w+)\.(\w+)\((.+?)\)',  # MongoDB operations
            r'(document|guild_doc|user_doc|server_doc)\[([\'\"]?\w+[\'\"]?)\]',  # Dict access
            r'if\s+(document|guild_doc|user_doc|server_doc)(\s*[=!]=\s*None|\s*is\s+None|\s*is\s+not\s+None)?:',  # Truthiness
            r'(document|guild_doc|user_doc|server_doc)\.get\(([\'\"]?\w+[\'\"]?),?\s*([^)]*)\)'  # get method
        ]
        
        self.premium_patterns = [
            r'@premium_required\(([\'\"]?\w+[\'\"]?),?\s*([^)]*)\)',  # Premium decorator
            r'verify_premium(?:_for_feature)?\((.*?)(?:,\s*feature_name=([\'\"]?\w+[\'\"]?))?(.*?)\)',  # Verify premium
            r'check_premium(?:_feature)?\((.*?)(?:,\s*feature_name=([\'\"]?\w+[\'\"]?))?(.*?)\)',  # Check premium
            r'has_premium_feature\(([\'\"]?\w+[\'\"]?)\)',  # Direct check
            r'premium_tier\s*[<>=]=?\s*(\d+)'  # Tier comparison
        ]
        
        self.get_error()_patterns = [
            r'try:.*?except\s+(\w+(?:\s*,\s*\w+)*)(?:\s+as\s+(\w+))?:',  # Try-except
            r'@error_handler',  # Error handler decorator
            r'on_command_error',  # Command error handler
            r'on_application_command_error'  # Application command error handler
        ]
    
    def analyze_file(self, file_path: str) -> None:
        """Analyze a single file for patterns"""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Skip if file is empty
        if not content.strip():
            return
        
        rel_path = os.path.relpath(file_path, self.project_root)
        self.results.total_files += 1
        
        # Find MongoDB access patterns
        for pattern in self.mongodb_patterns:
            for match in re.finditer(pattern, content, re.MULTILINE | re.DOTALL):
                line_num = content[:match.start()].count('\n') + 1
                pattern_type = self._determine_access_pattern_type(match.group())
                target = self._extract_access_target(match.group())
                context = self._get_context(content, match.start(), match.end())
                
                self.results.add_access_pattern(AccessPattern(
                    file=rel_path,
                    line=line_num,
                    pattern_type=pattern_type,
                    target=target,
                    context=context
                ))
        
        # Find premium verification patterns
        for pattern in self.premium_patterns:
            for match in re.finditer(pattern, content, re.MULTILINE | re.DOTALL):
                line_num = content[:match.start()].count('\n') + 1
                verification_method = self._determine_premium_check_type(match.group())
                feature_name = self._extract_feature_name(match.group())
                context = self._get_context(content, match.start(), match.end())
                
                self.results.add_premium_check(PremiumCheck(
                    file=rel_path,
                    line=line_num,
                    feature_name=feature_name if feature_name else "UNKNOWN",
                    verification_method=verification_method,
                    context=context
                ))
        
        # Find error handling patterns
        for pattern in self.get_error()_patterns:
            for match in re.finditer(pattern, content, re.MULTILINE | re.DOTALL):
                line_num = content[:match.start()].count('\n') + 1
                handler_type = self._determine_error_handler_type(match.group())
                exceptions = self._extract_exceptions(match.group())
                context = self._get_context(content, match.start(), match.end())
                
                self.results.add_error_handler(ErrorHandler(
                    file=rel_path,
                    line=line_num,
                    handler_type=handler_type,
                    exceptions=exceptions,
                    context=context
                ))
        
        # Count commands
        self.results.total_commands += len(re.findall(r'async def (?:cmd_|command_|on_)(\w+)', content))
    
    def _determine_access_pattern_type(self, pattern: str) -> str:
        """Determine the type of access pattern"""
        if '.get(' in pattern:
            return "get"
        elif 'try:' in pattern and 'except' in pattern:
            return "try_except"
        elif 'if ' in pattern and (' is ' in pattern or ' == ' in pattern or ' != ' in pattern):
            return "safe_comparison"
        elif re.search(r'\[([\'\"]?\w+[\'\"]?)\]', pattern):
            return "direct"
        else:
            return "truthiness"
    
    def _extract_access_target(self, pattern: str) -> str:
        """Extract the target being accessed"""
        direct_match = re.search(r'\[([\'\"]?(\w+)[\'\"]?)\]', pattern)
        get_match = re.search(r'\.get\(([\'\"]?(\w+)[\'\"]?)', pattern)
        mongodb_match = re.search(r'(?:db|self\.bot\.db|ctx\.bot\.db)\.(\w+)\.(\w+)', pattern)
        
        if direct_match is not None:
            return direct_match.group(2)
        elif get_match is not None:
            return get_match.group(2)
        elif mongodb_match is not None:
            return f"{mongodb_match.group(1)}.{mongodb_match.group(2)}"
        else:
            return "unknown"
    
    def _determine_premium_check_type(self, pattern: str) -> str:
        """Determine the type of premium check"""
        if pattern.startswith('@premium_required'):
            return "decorator"
        elif 'verify_premium' in pattern:
            return "utility"
        elif 'check_premium' in pattern:
            return "utility"
        elif 'has_premium_feature' in pattern:
            return "direct"
        else:
            return "comparison"
    
    def _extract_feature_name(self, pattern: str) -> Optional[str]:
        """Extract the feature name from a premium check"""
        decorator_match = re.search(r'@premium_required\(([\'\"](\w+)[\'\"])', pattern)
        feature_match = re.search(r'feature_name=([\'\"](\w+)[\'\"])', pattern)
        direct_match = re.search(r'has_premium_feature\(([\'\"](\w+)[\'\"])', pattern)
        
        if decorator_match is not None:
            return decorator_match.group(2)
        elif feature_match is not None:
            return feature_match.group(2)
        elif direct_match is not None:
            return direct_match.group(2)
        else:
            return None
    
    def _determine_error_handler_type(self, pattern: str) -> str:
        """Determine the type of error handler"""
        if pattern.startswith('try:'):
            return "try_except"
        elif pattern.startswith('@error_handler'):
            return "decorator"
        elif 'on_command_error' in pattern:
            return "command"
        else:
            return "application"
    
    def _extract_exceptions(self, pattern: str) -> List[str]:
        """Extract the exceptions being caught"""
        except_match = re.search(r'except\s+(\w+(?:\s*,\s*\w+)*)', pattern)
        if except_match is not None:
            return [e.strip() for e in except_match.group(1).split(',')]
        else:
            return ["Exception"]  # Default to catching all exceptions
    
    def _get_context(self, content: str, start: int, end: int) -> str:
        """Get the context around a match"""
        # Find the start of the line
        line_start = content.rfind('\n', 0, start) + 1
        if line_start == 0:
            line_start = 0
        
        # Find the end of the line
        line_end = content.find('\n', end)
        if line_end == -1:
            line_end = len(content)
        
        return content[line_start:line_end].strip()
    
    def analyze_project(self) -> AnalysisResults:
        """Analyze the entire project"""
        for root, _, files in os.walk(self.project_root):
            for file in files:
                if file.endswith('.py') and not file.startswith('.'):
                    file_path = os.path.join(root, file)
                    self.analyze_file(file_path)
        
        # Post-process results to identify inconsistencies
        self._identify_inconsistencies()
        
        return self.results
    
    def _identify_inconsistencies(self) -> None:
        """Identify inconsistencies in the results"""
        # Group premium checks by feature name
        premium_by_feature = {}
        for check in self.results.premium_checks:
            if check.feature_name not in premium_by_feature:
                premium_by_feature[check.feature_name] = []
            premium_by_feature[check.feature_name].append(check)
        
        # Check for inconsistent verification methods for the same feature
        for feature, checks in premium_by_feature.items():
            methods = set(check.verification_method for check in checks)
            if len(methods) > 1:
                logger.warning(f"Inconsistent verification methods for feature '{feature}': {methods}")
                self.results.inconsistent_premium_count += 1

def main():
    parser = argparse.ArgumentParser(description="Analyze Discord bot command pipeline")
    parser.add_argument("--root", type=str, default=".", help="Root directory of the project")
    parser.add_argument("--output", type=str, default="analysis_results.txt", help="Output file for analysis results")
    parser.add_argument("--verbose", action="store_true", help="Print verbose output")
    args = parser.parse_args()
    
    logger.info(f"Analyzing project at {args.root}")
    analyzer = CommandPipelineAnalyzer(args.root)
    results = analyzer.analyze_project()
    
    summary = results.summarize()
    
    with open(args.output, 'w', encoding='utf-8') as f:
        f.write("# Command Pipeline Analysis Results\n\n")
        
        f.write("## Summary\n\n")
        f.write(f"- Total files analyzed: {summary['total_files']}\n")
        f.write(f"- Total commands found: {summary['total_commands']}\n")
        f.write(f"- Unsafe database access patterns: {summary['unsafe_access_count']}\n")
        f.write(f"- Files with unsafe access: {summary['files_with_unsafe_access']}\n")
        f.write(f"- Unique premium features: {summary['unique_premium_features']}\n")
        f.write(f"- Premium verification methods: {', '.join(summary['premium_verification_methods'])}\n")
        f.write(f"- Inconsistent premium checks: {summary['inconsistent_premium_count']}\n")
        f.write(f"- Missing error handlers: {summary['missing_error_handler_count']}\n\n")
        
        if args.verbose is not None:
            f.write("## Unsafe Access Patterns\n\n")
            for pattern in results.access_patterns:
                if pattern.pattern_type in ["direct", "truthiness"]:
                    f.write(f"- {pattern}\n  Context: `{pattern.context}`\n\n")
            
            f.write("## Inconsistent Premium Checks\n\n")
            premium_by_feature = {}
            for check in results.premium_checks:
                if check.feature_name not in premium_by_feature:
                    premium_by_feature[check.feature_name] = []
                premium_by_feature[check.feature_name].append(check)
            
            for feature, checks in premium_by_feature.items():
                methods = set(check.verification_method for check in checks)
                if len(methods) > 1:
                    f.write(f"### Feature: {feature}\n\n")
                    for check in checks:
                        f.write(f"- {check.file}:{check.line} - {check.verification_method}\n  Context: `{check.context}`\n\n")
    
    logger.info(ff"\1")
    
    # Print summary to console
    print("\nCommand Pipeline Analysis Summary")
    print("================================")
    print(f"Total files analyzed: {summary['total_files']}")
    print(f"Total commands found: {summary['total_commands']}")
    print(f"Unsafe database access patterns: {summary['unsafe_access_count']}")
    print(f"Files with unsafe access: {summary['files_with_unsafe_access']}")
    print(f"Unique premium features: {summary['unique_premium_features']}")
    print(f"Premium verification methods: {', '.join(summary['premium_verification_methods'])}")
    print(f"Inconsistent premium checks: {summary['inconsistent_premium_count']}")
    print(f"Missing error handlers: {summary['missing_error_handler_count']}")

if __name__ == "__main__":
    main()