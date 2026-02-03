import { HelpCircle } from "lucide-react";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "./tooltip";

interface FieldTooltipProps {
  content: string;
}

export function FieldTooltip({ content }: FieldTooltipProps) {
  return (
    <TooltipProvider delayDuration={200}>
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            className="inline-flex items-center justify-center p-0.5 rounded-full text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
            tabIndex={-1}
          >
            <HelpCircle className="w-3.5 h-3.5" />
            <span className="sr-only">Help</span>
          </button>
        </TooltipTrigger>
        <TooltipContent 
          side="right" 
          align="center"
          sideOffset={8}
          collisionPadding={16}
          className="max-w-xs text-sm z-[10001]"
        >
          {content}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}
