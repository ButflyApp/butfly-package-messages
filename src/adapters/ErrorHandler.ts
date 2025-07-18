import { ValidationError } from "class-validator";
import { Logger } from "winston";

export class AppError extends Error {
  public readonly statusCode: number;
  public readonly errors?: ValidationError[];

  constructor(message: string, statusCode = 500, errors?: ValidationError[]) {
    super(message);
    this.statusCode = statusCode;
    this.errors = errors;
  }
}

const extractValidationErrors = (validationErrors: ValidationError[]): string[] => {
  const errors: string[] = [];

  const processError = (error: ValidationError, path: string = '') => {
    const currentPath = path ? `${path}.${error.property}` : error.property;

    if (error.constraints) {
      Object.entries(error.constraints).forEach(([constraint, message]) => {
        errors.push((message as string).replace(error.property, currentPath));
      });
    }
    if (error.children && error.children.length > 0) {
      error.children.forEach((childError: ValidationError) => {
        processError(childError, currentPath);
      });
    }
  };

  validationErrors.forEach(error => processError(error));
  return errors;
};

export class ErrorHandler {
  constructor(private readonly logger: Logger) {}

  handleError(error: Error, context: any = {}): {
    message: string;
    errors: string[];
    statusCode: number;
    stack?: string;
  } {
    const errorMessage = error.message.substring(0, 120).replace(/(\r\n|\n|\r)/gm, "");
    
    const logData: any = {
      error: error.stack,
      context,
    };

    if (error instanceof AppError && error.errors) {
      logData.validationErrors = error.errors.map(error => ({
        property: error.property,
        constraints: error.constraints,
        value: error.value
      }));
    }

    this.logger.error(`Error occurred: ${errorMessage}`, logData);

    if (error instanceof AppError) {
      const errors: string[] = error.errors
        ? extractValidationErrors(error.errors)
        : [error.message];

      return {
        message: errorMessage,
        errors,
        statusCode: error.statusCode,
        stack: error.stack
      };
    }

    return {
      message: "Internal server error",
      errors: ["Internal server error"],
      statusCode: 500,
      stack: error.stack
    };
  }
} 